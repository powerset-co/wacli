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

type BackfillOptions struct {
	ChatJID        string
	Count          int
	Requests       int
	WaitPerRequest time.Duration
	RequestDelay   time.Duration
	IdleExit       time.Duration
}

const (
	DefaultBackfillCount    = 50
	DefaultBackfillRequests = 1
	MaxBackfillCount        = 500
	MaxBackfillRequests     = 100
)

type BackfillResult struct {
	ChatJID            string
	RequestsSent       int
	ResponsesSeen      int
	MessagesReceived   int64
	MessagesAdded      int64
	MessagesSynced     int64
	OtherMessagesAdded int64
}

type onDemandResponse struct {
	conversations int
	messages      int
	endType       waHistorySync.Conversation_EndOfHistoryTransferType
}

func (a *App) BackfillHistory(ctx context.Context, opts BackfillOptions) (BackfillResult, error) {
	chatStr := strings.TrimSpace(opts.ChatJID)
	if chatStr == "" {
		return BackfillResult{}, fmt.Errorf("--chat is required")
	}
	chat, err := types.ParseJID(chatStr)
	if err != nil {
		return BackfillResult{}, fmt.Errorf("parse chat JID: %w", err)
	}
	chatStr = chat.String()

	opts = normalizeBackfillOptions(opts)
	if err := validateBackfillOptions(opts); err != nil {
		return BackfillResult{}, err
	}

	if err := a.EnsureAuthed(); err != nil {
		return BackfillResult{}, err
	}
	if err := a.OpenWA(); err != nil {
		return BackfillResult{}, err
	}
	a.wa.SetManualHistorySyncDownload(true)
	defer a.wa.SetManualHistorySyncDownload(false)

	beforeCount, err := a.db.CountChatMessages(chatStr)
	if err != nil {
		return BackfillResult{}, fmt.Errorf("count messages for %s before backfill: %w", chatStr, err)
	}
	beforeTotal, err := a.db.CountMessages()
	if err != nil {
		return BackfillResult{}, fmt.Errorf("count all messages before backfill: %w", err)
	}

	var mu sync.Mutex
	var waitCh chan onDemandResponse
	var manualMessagesStored atomic.Int64
	var manualLastEvent atomic.Int64
	manualLastEvent.Store(nowUTC().UnixNano())
	handleOnDemand := func(hs *events.HistorySync) {
		if hs == nil || hs.Data == nil || hs.Data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
			return
		}
		for _, conv := range hs.Data.GetConversations() {
			if strings.TrimSpace(conv.GetID()) != chatStr {
				continue
			}
			mu.Lock()
			ch := waitCh
			mu.Unlock()
			if ch == nil {
				return
			}
			resp := onDemandResponse{
				conversations: len(hs.Data.GetConversations()),
				messages:      len(conv.GetMessages()),
				endType:       conv.GetEndOfHistoryTransferType(),
			}
			select {
			case ch <- resp:
			default:
			}
			return
		}
	}
	handlerID := a.wa.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.HistorySync:
			if v == nil || v.Data == nil || v.Data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
				return
			}
			a.handleHistorySync(ctx, SyncOptions{}, v, &manualMessagesStored, &manualLastEvent, func(string, string) {})
			handleOnDemand(v)
		case *events.Message:
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
			handleOnDemand(hs)
		}
	})
	defer a.wa.RemoveEventHandler(handlerID)

	var requestsSent int
	var responsesSeen int
	var messagesReceived int64

	syncRes, err := a.Sync(ctx, SyncOptions{
		Mode:                SyncModeOnce,
		AllowQR:             false,
		IdleExit:            opts.IdleExit,
		SkipOnDemandHistory: true,
		AfterConnect: func(ctx context.Context) error {
			for i := 0; i < opts.Requests; i++ {
				if i > 0 && opts.RequestDelay > 0 {
					a.emitOrPrint("backfill_throttled", map[string]any{
						"chat_jid": chatStr,
						"delay":    opts.RequestDelay.String(),
						"request":  i + 1,
					}, "Waiting %s before the next history request...\n", opts.RequestDelay)
					timer := time.NewTimer(opts.RequestDelay)
					select {
					case <-ctx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						return ctx.Err()
					case <-timer.C:
					}
				}

				oldest, err := a.db.GetOldestMessageInfo(chatStr)
				if err != nil {
					if err == sql.ErrNoRows {
						return fmt.Errorf("no messages for %s in local DB; run `wacli sync` first", chatStr)
					}
					return err
				}

				reqInfo := types.MessageInfo{
					MessageSource: types.MessageSource{
						Chat:     chat,
						IsFromMe: oldest.FromMe,
					},
					ID:        types.MessageID(oldest.MsgID),
					Timestamp: oldest.Timestamp,
				}

				ch := make(chan onDemandResponse, 4)
				mu.Lock()
				waitCh = ch
				mu.Unlock()

				requestsSent++
				a.emitOrPrint("backfill_requesting", map[string]any{
					"chat_jid": chatStr,
					"count":    opts.Count,
					"request":  requestsSent,
				}, "Requesting %d older messages for %s...\n", opts.Count, chatStr)
				if _, err := a.wa.RequestHistorySyncOnDemand(ctx, reqInfo, opts.Count); err != nil {
					return err
				}

				var resp onDemandResponse
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resp = <-ch:
					responsesSeen++
					messagesReceived += int64(resp.messages)
				case <-time.After(opts.WaitPerRequest):
					return fmt.Errorf("timed out waiting for on-demand history sync response")
				}

				mu.Lock()
				if waitCh == ch {
					waitCh = nil
				}
				mu.Unlock()

				a.emitOrPrint("backfill_response", map[string]any{
					"chat_jid":       chatStr,
					"conversations":  resp.conversations,
					"messages":       resp.messages,
					"responses_seen": responsesSeen,
				}, "On-demand history sync: %d conversations, %d messages.\n", resp.conversations, resp.messages)

				newOldest, err := a.db.GetOldestMessageInfo(chatStr)
				if err == nil && newOldest.MsgID == oldest.MsgID {
					a.emitOrPrint("backfill_stopped", map[string]any{
						"chat_jid": chatStr,
						"reason":   "no_older_messages_added",
					}, "No older messages were added (stopping).\n")
					return nil
				}
				if resp.messages <= 0 {
					a.emitOrPrint("backfill_stopped", map[string]any{
						"chat_jid": chatStr,
						"reason":   "no_messages_returned",
					}, "No messages returned (stopping).\n")
					return nil
				}
				if resp.endType == waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY {
					a.emitOrPrint("backfill_stopped", map[string]any{
						"chat_jid": chatStr,
						"reason":   "start_of_history_reached",
					}, "Reached start of chat history (stopping).\n")
					return nil
				}
			}
			return nil
		},
	})
	if err != nil {
		return BackfillResult{}, err
	}

	afterCount, err := a.db.CountChatMessages(chatStr)
	if err != nil {
		return BackfillResult{}, fmt.Errorf("count messages for %s after backfill: %w", chatStr, err)
	}
	afterTotal, err := a.db.CountMessages()
	if err != nil {
		return BackfillResult{}, fmt.Errorf("count all messages after backfill: %w", err)
	}
	targetAdded := afterCount - beforeCount
	otherAdded := (afterTotal - beforeTotal) - targetAdded
	if otherAdded < 0 {
		otherAdded = 0
	}

	return BackfillResult{
		ChatJID:            chatStr,
		RequestsSent:       requestsSent,
		ResponsesSeen:      responsesSeen,
		MessagesReceived:   messagesReceived,
		MessagesAdded:      targetAdded,
		MessagesSynced:     syncRes.MessagesStored + manualMessagesStored.Load(),
		OtherMessagesAdded: otherAdded,
	}, nil
}

func normalizeBackfillOptions(opts BackfillOptions) BackfillOptions {
	if opts.Count <= 0 {
		opts.Count = DefaultBackfillCount
	}
	if opts.Requests <= 0 {
		opts.Requests = DefaultBackfillRequests
	}
	if opts.WaitPerRequest <= 0 {
		opts.WaitPerRequest = 60 * time.Second
	}
	if opts.IdleExit <= 0 {
		opts.IdleExit = 5 * time.Second
	}
	return opts
}

func validateBackfillOptions(opts BackfillOptions) error {
	if opts.Count > MaxBackfillCount {
		return fmt.Errorf("--count must be <= %d (got %d)", MaxBackfillCount, opts.Count)
	}
	if opts.Requests > MaxBackfillRequests {
		return fmt.Errorf("--requests must be <= %d (got %d)", MaxBackfillRequests, opts.Requests)
	}
	return nil
}
