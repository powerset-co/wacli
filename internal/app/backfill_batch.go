package app

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type BackfillBatchOptions struct {
	ChatJIDs       []string
	Count          int
	BatchSize      int
	MaxInFlight    int
	LIDFallback    bool
	Requests       int
	RequestDelay   time.Duration
	WaitPerBatch   time.Duration
	BatchDelay     time.Duration
	TimeoutBackoff time.Duration
	IdleExit       time.Duration
}

const DefaultBackfillBatchSize = 10

type BackfillBatchChatResult struct {
	ChatJID          string
	RequestsSent     int
	ResponsesSeen    int
	MessagesReceived int64
	MessagesAdded    int64
	RequestIdentity  string
	EndType          string
	Elapsed          time.Duration
	Error            string
}

type BackfillBatchResult struct {
	Chats              []BackfillBatchChatResult
	MessagesSynced     int64
	OtherMessagesAdded int64
	Elapsed            time.Duration
}

type batchBackfillTarget struct {
	chat              types.JID
	chatJID           string
	beforeCount       int64
	started           time.Time
	result            BackfillBatchChatResult
	lastResponse      onDemandResponse
	lastResponded     bool
	lastAdded         int64
	lastUseLID        bool
	preferredIdentity string
	initialIdentity   string
}

type batchBackfillRequest struct {
	id       string
	target   *batchBackfillTarget
	identity string
	useLID   bool
}

type batchHistoryResponse struct {
	request *batchBackfillRequest
	history *waHistorySync.HistorySync
}

type batchIncomingHistory struct {
	requestIDs []string
	history    *waHistorySync.HistorySync
}

// backfillBatchCoordinator owns the short-lived correlation state for one
// command. Exact request IDs are authoritative; ID-less responses can fall
// back to canonical chat plus PN/LID identity.
type backfillBatchCoordinator struct {
	app                *App
	ctx                context.Context
	maxEarlyResponses  int
	mu                 sync.Mutex
	pendingByRequestID map[string]*batchBackfillRequest
	pendingByIdentity  map[string]*batchBackfillRequest
	earlyResponses     []batchIncomingHistory
	responseReady      chan batchHistoryResponse
}

func newBackfillBatchCoordinator(
	app *App,
	ctx context.Context,
	targetCount int,
	opts BackfillBatchOptions,
) *backfillBatchCoordinator {
	maxEarly := opts.MaxInFlight
	if maxEarly < 1 {
		maxEarly = 1
	}
	return &backfillBatchCoordinator{
		app:                app,
		ctx:                ctx,
		maxEarlyResponses:  maxEarly,
		pendingByRequestID: make(map[string]*batchBackfillRequest, targetCount),
		pendingByIdentity:  make(map[string]*batchBackfillRequest, targetCount),
		earlyResponses:     make([]batchIncomingHistory, 0, maxEarly),
		responseReady:      make(chan batchHistoryResponse, targetCount*opts.Requests),
	}
}

func (c *backfillBatchCoordinator) identityKey(chatJID, identity string) string {
	return chatJID + "\x00" + identity
}

func (c *backfillBatchCoordinator) incomingIdentityKeys(
	history *waHistorySync.HistorySync,
) []string {
	keys := make([]string, 0, len(history.GetConversations()))
	seen := make(map[string]struct{}, len(history.GetConversations()))
	for _, conv := range history.GetConversations() {
		raw := strings.TrimSpace(conv.GetID())
		parsed, err := types.ParseJID(raw)
		if err != nil {
			continue
		}
		identity := "pn"
		if parsed.Server == types.HiddenUserServer {
			identity = "lid"
		}
		key := c.identityKey(c.app.canonicalHistoryChatJID(c.ctx, raw), identity)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func (c *backfillBatchCoordinator) incomingMatchesRequest(
	incoming batchIncomingHistory,
	request *batchBackfillRequest,
) bool {
	hasRequestID := false
	for _, requestID := range incoming.requestIDs {
		if requestID == "" {
			continue
		}
		hasRequestID = true
		if requestID == request.id {
			return true
		}
	}
	if hasRequestID {
		return false
	}
	wantedIdentity := c.identityKey(request.target.chatJID, request.identity)
	for _, key := range c.incomingIdentityKeys(incoming.history) {
		if key == wantedIdentity {
			return true
		}
	}
	return false
}

func (c *backfillBatchCoordinator) matchLocked(
	incoming batchIncomingHistory,
) *batchBackfillRequest {
	for _, request := range c.pendingByRequestID {
		if c.incomingMatchesRequest(incoming, request) {
			return request
		}
	}
	return nil
}

func (c *backfillBatchCoordinator) removeLocked(request *batchBackfillRequest) {
	if request == nil {
		return
	}
	delete(c.pendingByRequestID, request.id)
	delete(
		c.pendingByIdentity,
		c.identityKey(request.target.chatJID, request.identity),
	)
}

func (c *backfillBatchCoordinator) handle(
	notif *waE2E.HistorySyncNotification,
	history *waHistorySync.HistorySync,
) {
	if history == nil || history.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
		return
	}
	incoming := batchIncomingHistory{
		requestIDs: []string{
			strings.TrimSpace(notif.GetOriginalMessageID()),
			strings.TrimSpace(notif.GetPeerDataRequestSessionID()),
			strings.TrimSpace(
				notif.GetFullHistorySyncOnDemandRequestMetadata().GetRequestID(),
			),
		},
		history: history,
	}
	c.mu.Lock()
	request := c.matchLocked(incoming)
	if request == nil {
		if len(c.earlyResponses) >= c.maxEarlyResponses {
			c.earlyResponses = c.earlyResponses[1:]
		}
		c.earlyResponses = append(c.earlyResponses, incoming)
		c.mu.Unlock()
		return
	}
	c.removeLocked(request)
	c.mu.Unlock()
	c.responseReady <- batchHistoryResponse{request: request, history: history}
}

func (c *backfillBatchCoordinator) register(
	request *batchBackfillRequest,
) *batchIncomingHistory {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingByRequestID[request.id] = request
	c.pendingByIdentity[c.identityKey(request.target.chatJID, request.identity)] = request

	var matched *batchIncomingHistory
	for _, incoming := range c.earlyResponses {
		if !c.incomingMatchesRequest(incoming, request) {
			continue
		}
		copy := incoming
		matched = &copy
		c.removeLocked(request)
		break
	}
	// Every legitimate pre-registration response for this request has now had
	// a chance to match. Drop stale decrypted payloads after this short window.
	c.earlyResponses = nil
	return matched
}

func (c *backfillBatchCoordinator) remove(request *batchBackfillRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(request)
}

func (a *App) BackfillHistoryBatch(ctx context.Context, opts BackfillBatchOptions) (BackfillBatchResult, error) {
	started := time.Now()
	opts = normalizeBackfillBatchOptions(opts)
	if err := validateBackfillBatchOptions(opts); err != nil {
		return BackfillBatchResult{}, err
	}

	if err := a.EnsureAuthed(); err != nil {
		return BackfillBatchResult{}, err
	}
	if err := a.OpenWA(); err != nil {
		return BackfillBatchResult{}, err
	}
	a.wa.SetManualHistorySyncDownload(true)
	defer a.wa.SetManualHistorySyncDownload(false)

	targets, err := a.prepareBackfillBatchTargets(ctx, opts.ChatJIDs)
	if err != nil {
		return BackfillBatchResult{}, err
	}
	beforeTotal, err := a.db.CountMessages()
	if err != nil {
		return BackfillBatchResult{}, fmt.Errorf("count all messages before batch backfill: %w", err)
	}

	coordinator := newBackfillBatchCoordinator(a, ctx, len(targets), opts)
	var manualMessagesStored atomic.Int64
	var manualLastEvent atomic.Int64
	manualLastEvent.Store(nowUTC().UnixNano())
	handlerID := a.wa.AddEventHandler(func(evt interface{}) {
		v, ok := evt.(*events.Message)
		if !ok {
			return
		}
		notif := historySyncNotificationFromMessage(v)
		if notif == nil || notif.GetSyncType() != waE2E.HistorySyncType_ON_DEMAND {
			return
		}
		data, err := a.wa.DownloadHistorySync(ctx, notif)
		if err != nil {
			a.emitWarning(
				"on_demand_history_download_failed",
				fmt.Sprintf("warning: failed to download on-demand history sync: %v", err),
				map[string]any{"error": err.Error()},
			)
			return
		}
		if data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
			return
		}
		hs := &events.HistorySync{Data: data}
		a.handleHistorySync(ctx, SyncOptions{}, hs, &manualMessagesStored, &manualLastEvent, func(string, string) {})
		if err := a.wa.DeleteHistorySyncMedia(ctx, notif); err != nil {
			a.emitWarning(
				"history_delete_failed",
				fmt.Sprintf("warning: failed to delete on-demand history sync media: %v", err),
				map[string]any{"error": err.Error()},
			)
		}
		coordinator.handle(notif, data)
	})
	defer a.wa.RemoveEventHandler(handlerID)

	syncRes, err := a.Sync(ctx, SyncOptions{
		Mode:                SyncModeOnce,
		AllowQR:             false,
		IdleExit:            opts.IdleExit,
		SkipOnDemandHistory: true,
		AfterConnect: func(syncCtx context.Context) error {
			runWave := func(
				wave []*batchBackfillTarget,
				batchNumber int,
				batchSize int,
				useLID bool,
			) error {
				requested := make([]*batchBackfillRequest, 0, len(wave))
				beforeWave := make(map[*batchBackfillTarget]int64, len(wave))
				for _, target := range wave {
					if target.result.RequestsSent >= opts.Requests {
						continue
					}
					if target.started.IsZero() {
						target.started = time.Now()
					}
					target.result.ChatJID = target.chatJID
					target.result.Error = ""
					target.lastResponded = false
					target.lastAdded = 0
					currentCount, err := a.db.CountChatMessages(target.chatJID)
					if err != nil {
						target.result.Error = err.Error()
						target.result.Elapsed = time.Since(target.started)
						continue
					}
					beforeWave[target] = currentCount
					oldest, err := a.db.GetOldestMessageInfo(target.chatJID)
					if err != nil {
						if err == sql.ErrNoRows {
							target.result.Error = "no local message anchor"
						} else {
							target.result.Error = err.Error()
						}
						target.result.Elapsed = time.Since(target.started)
						continue
					}
					requestChat := target.chat
					if useLID && requestChat.Server != types.HiddenUserServer {
						requestChat = a.wa.ResolvePNToLID(syncCtx, requestChat)
						if requestChat == target.chat {
							target.result.Error = "no distinct LID mapping"
							target.result.Elapsed = time.Since(target.started)
							continue
						}
					}
					reqInfo := types.MessageInfo{
						MessageSource: types.MessageSource{
							Chat:     requestChat,
							IsFromMe: oldest.FromMe,
						},
						ID:        types.MessageID(oldest.MsgID),
						Timestamp: oldest.Timestamp,
					}
					requestID, err := a.wa.RequestHistorySyncOnDemand(syncCtx, reqInfo, opts.Count)
					if err != nil {
						target.result.Error = err.Error()
						target.result.Elapsed = time.Since(target.started)
						continue
					}
					request := &batchBackfillRequest{
						id:       string(requestID),
						target:   target,
						identity: map[bool]string{true: "lid", false: "pn"}[useLID],
						useLID:   useLID,
					}
					if strings.TrimSpace(request.id) == "" {
						target.result.Error = "history request returned no request ID"
						target.result.Elapsed = time.Since(target.started)
						continue
					}
					target.result.RequestsSent++
					target.result.RequestIdentity = request.identity
					target.lastUseLID = useLID
					requested = append(requested, request)
					early := coordinator.register(request)
					if early != nil {
						coordinator.responseReady <- batchHistoryResponse{
							request: request,
							history: early.history,
						}
					}
					a.emitOrPrint("backfill_batch_requesting", map[string]any{
						"chat_jid":         target.chatJID,
						"count":            opts.Count,
						"batch":            batchNumber,
						"batch_size":       batchSize,
						"max_inflight":     opts.MaxInFlight,
						"request_identity": request.identity,
					}, "Requesting %d older messages for %s...\n", opts.Count, target.chatJID)
				}

				unresolved := make(map[*batchBackfillRequest]struct{}, len(requested))
				for _, request := range requested {
					unresolved[request] = struct{}{}
				}
				timer := time.NewTimer(opts.WaitPerBatch)
			collectResponses:
				for len(unresolved) > 0 {
					select {
					case <-syncCtx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						return syncCtx.Err()
					case received := <-coordinator.responseReady:
						request := received.request
						if _, ok := unresolved[request]; !ok {
							continue
						}
						target := request.target
						var resp onDemandResponse
						matched := false
						for _, conv := range received.history.GetConversations() {
							if a.canonicalHistoryChatJID(syncCtx, conv.GetID()) != target.chatJID {
								continue
							}
							resp = onDemandResponse{
								conversations: len(received.history.GetConversations()),
								messages:      len(conv.GetMessages()),
								endType:       conv.GetEndOfHistoryTransferType(),
								hasEndType:    conv.EndOfHistoryTransferType != nil,
							}
							matched = true
							break
						}
						if !matched {
							target.result.Error = "history response did not include requested chat"
							target.result.Elapsed = time.Since(target.started)
							delete(unresolved, request)
							continue
						}
						target.result.ResponsesSeen++
						target.result.MessagesReceived += int64(resp.messages)
						target.result.RequestIdentity = request.identity
						if resp.hasEndType {
							target.result.EndType = resp.endType.String()
						} else {
							target.result.EndType = ""
						}
						target.result.Error = ""
						target.result.Elapsed = time.Since(target.started)
						target.lastResponse = resp
						target.lastResponded = true
						target.lastUseLID = request.useLID
						// The first response proves a route works. A later
						// alternate route replaces it only when that response
						// actually contains history, avoiding preference churn
						// when both identities return an empty terminal result.
						if target.result.ResponsesSeen == 1 || resp.messages > 0 {
							target.preferredIdentity = request.identity
							if err := a.db.SetHistoryRequestIdentity(target.chatJID, request.identity); err != nil {
								a.emitWarning(
									"history_request_identity_save_failed",
									fmt.Sprintf("warning: failed to save history request identity: %v", err),
									map[string]any{"error": err.Error()},
								)
							}
						}
						delete(unresolved, request)
						a.emitOrPrint("backfill_batch_response", map[string]any{
							"chat_jid":         target.chatJID,
							"messages":         resp.messages,
							"request_identity": target.result.RequestIdentity,
						}, "On-demand history sync for %s: %d messages.\n", target.chatJID, resp.messages)
					case <-timer.C:
						break collectResponses
					}
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				for request := range unresolved {
					target := request.target
					coordinator.remove(request)
					if target.result.ResponsesSeen == 0 {
						target.result.Error = "timed out waiting for on-demand history sync response"
					} else {
						// A later optional retry timing out must not erase an
						// earlier correlated response and recovered rows.
						target.result.Error = ""
					}
					target.result.Elapsed = time.Since(target.started)
				}

				for _, request := range requested {
					target := request.target
					coordinator.remove(request)
					afterCount, err := a.db.CountChatMessages(target.chatJID)
					if err != nil && target.result.Error == "" {
						target.result.Error = err.Error()
					}
					target.lastAdded = afterCount - beforeWave[target]
					target.result.MessagesAdded = afterCount - target.beforeCount
					if target.result.Elapsed == 0 {
						target.result.Elapsed = time.Since(target.started)
					}
				}
				return nil
			}

			runIdentityWaves := func(
				selected []*batchBackfillTarget,
				batchNumber int,
				batchSize int,
				useLID bool,
			) error {
				for waveOffset := 0; waveOffset < len(selected); waveOffset += opts.MaxInFlight {
					waveEnd := waveOffset + opts.MaxInFlight
					if waveEnd > len(selected) {
						waveEnd = len(selected)
					}
					if err := runWave(
						selected[waveOffset:waveEnd],
						batchNumber,
						batchSize,
						useLID,
					); err != nil {
						return err
					}
				}
				return nil
			}

			for offset := 0; offset < len(targets); offset += opts.BatchSize {
				end := offset + opts.BatchSize
				if end > len(targets) {
					end = len(targets)
				}
				batch := targets[offset:end]
				responsesBefore := 0
				for _, target := range batch {
					responsesBefore += target.result.ResponsesSeen
				}

				for _, useLID := range []bool{false, true} {
					initial := make([]*batchBackfillTarget, 0, len(batch))
					for _, target := range batch {
						preferLID := target.chat.Server == types.HiddenUserServer ||
							target.preferredIdentity == "lid"
						if preferLID && !a.canUseLIDHistoryIdentity(syncCtx, target) {
							preferLID = false
						}
						if preferLID == useLID {
							initial = append(initial, target)
						}
					}
					if err := runIdentityWaves(
						initial,
						offset/opts.BatchSize+1,
						len(batch),
						useLID,
					); err != nil {
						return err
					}
				}

				if opts.LIDFallback {
					for _, fallbackUseLID := range []bool{false, true} {
						fallback := make([]*batchBackfillTarget, 0, len(batch))
						for _, target := range batch {
							if target.result.RequestsSent == 0 ||
								target.result.RequestsSent >= opts.Requests ||
								target.lastUseLID == fallbackUseLID {
								continue
							}
							lastIdentity := map[bool]string{true: "lid", false: "pn"}[target.lastUseLID]
							if target.lastResponded &&
								target.initialIdentity != "" &&
								target.initialIdentity == lastIdentity {
								// A route proven by an earlier run responded
								// again. Do not re-probe the alternate identity
								// merely because this rerun added no new rows.
								continue
							}
							needsAlternate := (!target.lastResponded &&
								strings.Contains(target.result.Error, "timed out")) ||
								(target.lastResponded &&
									(target.lastAdded <= 0 || target.lastResponse.messages == 0))
							if !needsAlternate {
								continue
							}
							if fallbackUseLID &&
								!a.canUseLIDHistoryIdentity(syncCtx, target) {
								continue
							}
							if !fallbackUseLID &&
								target.chat.Server != types.DefaultUserServer {
								continue
							}
							fallback = append(fallback, target)
						}
						if err := runIdentityWaves(
							fallback,
							offset/opts.BatchSize+1,
							len(batch),
							fallbackUseLID,
						); err != nil {
							return err
						}
					}
				}

				if opts.Requests > 1 {
					for {
						active := make([]*batchBackfillTarget, 0, len(batch))
						for _, target := range batch {
							moreAvailable := target.lastResponse.hasEndType &&
								(target.lastResponse.endType ==
									waHistorySync.Conversation_COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY ||
									target.lastResponse.endType ==
										waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY)
							if target.lastResponded &&
								target.lastAdded > 0 &&
								target.lastResponse.messages > 0 &&
								moreAvailable &&
								target.result.RequestsSent < opts.Requests {
								active = append(active, target)
							}
						}
						if len(active) == 0 {
							break
						}

						if opts.RequestDelay > 0 {
							timer := time.NewTimer(opts.RequestDelay)
							select {
							case <-syncCtx.Done():
								if !timer.Stop() {
									<-timer.C
								}
								return syncCtx.Err()
							case <-timer.C:
							}
						}

						for _, useLID := range []bool{false, true} {
							identityTargets := make([]*batchBackfillTarget, 0, len(active))
							for _, target := range active {
								if target.lastUseLID == useLID {
									identityTargets = append(identityTargets, target)
								}
							}
							if err := runIdentityWaves(
								identityTargets,
								offset/opts.BatchSize+1,
								len(batch),
								useLID,
							); err != nil {
								return err
							}
						}
					}
				}

				if end < len(targets) {
					delay := opts.BatchDelay
					reason := "batch_complete"
					responsesAfter := 0
					for _, target := range batch {
						responsesAfter += target.result.ResponsesSeen
					}
					if responsesAfter == responsesBefore &&
						opts.TimeoutBackoff > delay {
						delay = opts.TimeoutBackoff
						reason = "no_responses"
					}
					if delay <= 0 {
						continue
					}
					a.emitOrPrint("backfill_batch_throttled", map[string]any{
						"delay":  delay.String(),
						"batch":  offset/opts.BatchSize + 1,
						"reason": reason,
					}, "Waiting %s before the next history batch (%s)...\n", delay, reason)
					timer := time.NewTimer(delay)
					select {
					case <-syncCtx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						return syncCtx.Err()
					case <-timer.C:
					}
				}
			}
			return nil
		},
	})
	if err != nil {
		return BackfillBatchResult{}, err
	}

	afterTotal, err := a.db.CountMessages()
	if err != nil {
		return BackfillBatchResult{}, fmt.Errorf("count all messages after batch backfill: %w", err)
	}
	results := make([]BackfillBatchChatResult, 0, len(targets))
	var targetAdded int64
	for _, target := range targets {
		results = append(results, target.result)
		targetAdded += target.result.MessagesAdded
	}
	otherAdded := afterTotal - beforeTotal - targetAdded
	if otherAdded < 0 {
		otherAdded = 0
	}
	return BackfillBatchResult{
		Chats:              results,
		MessagesSynced:     syncRes.MessagesStored + manualMessagesStored.Load(),
		OtherMessagesAdded: otherAdded,
		Elapsed:            time.Since(started),
	}, nil
}

func (a *App) prepareBackfillBatchTargets(
	ctx context.Context,
	chatJIDs []string,
) ([]*batchBackfillTarget, error) {
	seen := make(map[string]struct{}, len(chatJIDs))
	targets := make([]*batchBackfillTarget, 0, len(chatJIDs))
	for _, raw := range chatJIDs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		chat, err := types.ParseJID(raw)
		if err != nil {
			return nil, fmt.Errorf("parse chat JID %q: %w", raw, err)
		}
		chat = chat.ToNonAD()
		inputWasLID := chat.Server == types.HiddenUserServer
		if inputWasLID {
			if resolved := a.wa.ResolveLIDToPN(ctx, chat); !resolved.IsEmpty() {
				chat = resolved.ToNonAD()
			}
		}
		chatJID := chat.String()
		if _, ok := seen[chatJID]; ok {
			continue
		}
		seen[chatJID] = struct{}{}
		beforeCount, err := a.db.CountChatMessages(chatJID)
		if err != nil {
			return nil, fmt.Errorf("count messages for %s before batch backfill: %w", chatJID, err)
		}
		preferredIdentity, err := a.db.GetHistoryRequestIdentity(chatJID)
		if err != nil {
			return nil, fmt.Errorf("get history request identity for %s: %w", chatJID, err)
		}
		if preferredIdentity == "" && inputWasLID {
			preferredIdentity = "lid"
		}
		targets = append(targets, &batchBackfillTarget{
			chat:              chat,
			chatJID:           chatJID,
			beforeCount:       beforeCount,
			preferredIdentity: preferredIdentity,
			initialIdentity:   preferredIdentity,
		})
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("at least one --chat is required")
	}
	return targets, nil
}

func (a *App) canonicalHistoryChatJID(ctx context.Context, raw string) string {
	chat, err := types.ParseJID(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw)
	}
	chat = chat.ToNonAD()
	if chat.Server == types.HiddenUserServer {
		if resolved := a.wa.ResolveLIDToPN(ctx, chat); !resolved.IsEmpty() {
			chat = resolved.ToNonAD()
		}
	}
	return chat.String()
}

func (a *App) canUseLIDHistoryIdentity(
	ctx context.Context,
	target *batchBackfillTarget,
) bool {
	if target == nil || target.chat.Server != types.DefaultUserServer {
		return target != nil && target.chat.Server == types.HiddenUserServer
	}
	lid := a.wa.ResolvePNToLID(ctx, target.chat)
	return !lid.IsEmpty() && lid.Server == types.HiddenUserServer && lid != target.chat
}

func normalizeBackfillBatchOptions(opts BackfillBatchOptions) BackfillBatchOptions {
	if opts.Count <= 0 {
		opts.Count = DefaultBackfillCount
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBackfillBatchSize
	}
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = opts.BatchSize
	}
	if opts.MaxInFlight > opts.BatchSize {
		opts.MaxInFlight = opts.BatchSize
	}
	if opts.Requests <= 0 {
		opts.Requests = 1
	}
	if opts.WaitPerBatch <= 0 {
		opts.WaitPerBatch = 60 * time.Second
	}
	if opts.TimeoutBackoff <= 0 {
		opts.TimeoutBackoff = time.Minute
	}
	if opts.IdleExit <= 0 {
		opts.IdleExit = 5 * time.Second
	}
	return opts
}

func validateBackfillBatchOptions(opts BackfillBatchOptions) error {
	if opts.Count > MaxBackfillCount {
		return fmt.Errorf("--count must be <= %d (got %d)", MaxBackfillCount, opts.Count)
	}
	if opts.BatchSize > MaxBackfillRequests {
		return fmt.Errorf("--batch-size must be <= %d (got %d)", MaxBackfillRequests, opts.BatchSize)
	}
	if opts.MaxInFlight > opts.BatchSize {
		return fmt.Errorf("--max-inflight must be <= --batch-size")
	}
	if opts.Requests > MaxBackfillRequests {
		return fmt.Errorf("--requests must be <= %d (got %d)", MaxBackfillRequests, opts.Requests)
	}
	return nil
}
