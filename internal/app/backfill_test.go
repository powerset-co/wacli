package app

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/powerset-co/wacli/internal/store"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestBackfillHistoryAddsOlderMessages(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	chatStr := chat.String()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := a.db.UpsertChat(chatStr, "dm", "Alice", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(chatStr, "m2", base.Add(2*time.Second), "newer")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(chatStr),
				FromMe:    proto.Bool(false),
				ID:        proto.String("m1"),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Add(1 * time.Second).Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		return &events.HistorySync{
			Data: &waHistorySync.HistorySync{
				SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
				Conversations: []*waHistorySync.Conversation{{
					ID:                       proto.String(chatStr),
					EndOfHistoryTransfer:     proto.Bool(true),
					EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum(),
					Messages:                 []*waHistorySync.HistorySyncMsg{{Message: older}},
				}},
			},
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := a.BackfillHistory(ctx, BackfillOptions{
		ChatJID:        chatStr,
		Count:          50,
		Requests:       1,
		WaitPerRequest: 1 * time.Second,
		IdleExit:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistory: %v", err)
	}
	if res.MessagesAdded <= 0 {
		t.Fatalf("expected messages to be added, got %d", res.MessagesAdded)
	}
	if res.MessagesReceived != 1 || res.MessagesSynced != 1 {
		t.Fatalf("received/synced = %d/%d, want 1/1", res.MessagesReceived, res.MessagesSynced)
	}
	if res.OtherMessagesAdded != 0 {
		t.Fatalf("other added = %d, want 0", res.OtherMessagesAdded)
	}

	oldest, err := a.db.GetOldestMessageInfo(chatStr)
	if err != nil {
		t.Fatalf("GetOldestMessageInfo: %v", err)
	}
	if oldest.MsgID != "m1" {
		t.Fatalf("expected oldest m1, got %q", oldest.MsgID)
	}
	if got := f.manualHistorySyncCalls; len(got) != 4 || !got[0] || !got[1] || got[2] || got[3] {
		t.Fatalf("manual history sync calls = %v, want [true true false false]", got)
	}
}

func TestBackfillHistoryPersistsAsyncResponseBeforeEvaluatingProgress(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	chatStr := chat.String()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chatStr, "dm", "Async Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(chatStr, "current", base.Add(2*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	older := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: proto.String(chatStr),
			FromMe:    proto.Bool(false),
			ID:        proto.String("older-async"),
		},
		MessageTimestamp: proto.Uint64(uint64(base.Unix())),
		Message:          &waProto.Message{Conversation: proto.String("older")},
	}
	f.onDemandHistory = func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:       proto.String(chatStr),
				Messages: []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}}
	}
	f.onDemandAsync = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := a.BackfillHistory(ctx, BackfillOptions{
		ChatJID:        chatStr,
		Count:          50,
		Requests:       1,
		WaitPerRequest: time.Second,
		IdleExit:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistory: %v", err)
	}
	if res.MessagesAdded != 1 || res.MessagesSynced != 1 {
		t.Fatalf("added/synced = %d/%d, want 1/1", res.MessagesAdded, res.MessagesSynced)
	}
	oldest, err := a.db.GetOldestMessageInfo(chatStr)
	if err != nil || oldest.MsgID != "older-async" {
		t.Fatalf("oldest = %+v, err = %v, want older-async", oldest, err)
	}
}

func TestBackfillHistoryDownloadsManualOnDemandNotification(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	chatStr := chat.String()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := a.db.UpsertChat(chatStr, "dm", "Alice", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(chatStr, "m2", base.Add(2*time.Second), "newer")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	syncType := waE2E.HistorySyncType_ON_DEMAND
	notif := &waE2E.HistorySyncNotification{SyncType: &syncType}
	f.onDemandEvent = func(lastKnown types.MessageInfo, count int) interface{} {
		return &events.Message{
			Message: &waProto.Message{
				ProtocolMessage: &waProto.ProtocolMessage{
					HistorySyncNotification: notif,
				},
			},
		}
	}
	downloadCalls := 0
	f.downloadHistory = func(got *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		downloadCalls++
		if got != notif {
			t.Fatalf("DownloadHistorySync notification = %p, want %p", got, notif)
		}
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(chatStr),
				FromMe:    proto.Bool(false),
				ID:        proto.String("m1"),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Add(1 * time.Second).Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		return &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:                       proto.String(chatStr),
				EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum(),
				Messages:                 []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := a.BackfillHistory(ctx, BackfillOptions{
		ChatJID:        chatStr,
		Count:          50,
		Requests:       1,
		WaitPerRequest: 1 * time.Second,
		IdleExit:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistory: %v", err)
	}
	if downloadCalls != 1 {
		t.Fatalf("download calls = %d, want 1", downloadCalls)
	}
	if got := len(f.deleteHistoryCalls); got != 1 || f.deleteHistoryCalls[0] != notif {
		t.Fatalf("delete history calls = %d, want one call for downloaded notification", got)
	}
	if res.MessagesAdded <= 0 {
		t.Fatalf("expected messages to be added, got %d", res.MessagesAdded)
	}
}

func TestBackfillHistoryCountsOnlyTargetChatMessages(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	target := types.JID{User: "123", Server: types.DefaultUserServer}
	targetStr := target.String()
	unrelated := types.JID{User: "456", Server: types.DefaultUserServer}
	unrelatedStr := unrelated.String()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := a.db.UpsertChat(targetStr, "dm", "Target Contact", base); err != nil {
		t.Fatalf("UpsertChat target: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(targetStr, "target-current", base.Add(2*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage target: %v", err)
	}

	unrelatedMessage := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: proto.String(unrelatedStr),
			FromMe:    proto.Bool(false),
			ID:        proto.String("unrelated-catchup"),
		},
		MessageTimestamp: proto.Uint64(uint64(base.Add(3 * time.Second).Unix())),
		Message:          &waProto.Message{Conversation: proto.String("unrelated")},
	}
	f.connectEvents = []interface{}{&events.HistorySync{Data: &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Conversations: []*waHistorySync.Conversation{{
			ID:       proto.String(unrelatedStr),
			Messages: []*waHistorySync.HistorySyncMsg{{Message: unrelatedMessage}},
		}},
	}}}
	f.onDemandHistory = func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:       proto.String(targetStr),
				Messages: nil,
			}},
		}}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := a.BackfillHistory(ctx, BackfillOptions{
		ChatJID:        targetStr,
		Count:          50,
		Requests:       1,
		WaitPerRequest: time.Second,
		IdleExit:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistory: %v", err)
	}
	if res.MessagesAdded != 0 {
		t.Fatalf("MessagesAdded = %d, want 0 target-chat messages", res.MessagesAdded)
	}
	if res.MessagesReceived != 0 {
		t.Fatalf("target received = %d, want 0", res.MessagesReceived)
	}
	if res.MessagesSynced != 1 || res.OtherMessagesAdded != 1 {
		t.Fatalf("all synced/other added = %d/%d, want 1/1", res.MessagesSynced, res.OtherMessagesAdded)
	}
	if got, err := a.db.CountChatMessages(unrelatedStr); err != nil || got != 1 {
		t.Fatalf("unrelated message count = %d, err = %v, want 1", got, err)
	}
}

func TestBackfillHistoryDelaysBetweenRequests(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	chatStr := chat.String()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chatStr, "dm", "Test Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(chatStr, "m3", base.Add(3*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	call := 0
	f.onDemandHistory = func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		call++
		id := fmt.Sprintf("m%d", 3-call)
		ts := base.Add(time.Duration(3-call) * time.Second)
		message := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(chatStr),
				FromMe:    proto.Bool(false),
				ID:        proto.String(id),
			},
			MessageTimestamp: proto.Uint64(uint64(ts.Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		endType := waHistorySync.Conversation_COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY
		if call == 2 {
			endType = waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:                       proto.String(chatStr),
				EndOfHistoryTransferType: endType.Enum(),
				Messages:                 []*waHistorySync.HistorySyncMsg{{Message: message}},
			}},
		}}
	}

	delay := 30 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := a.BackfillHistory(ctx, BackfillOptions{
		ChatJID:        chatStr,
		Count:          50,
		Requests:       2,
		WaitPerRequest: time.Second,
		RequestDelay:   delay,
		IdleExit:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistory: %v", err)
	}
	if res.RequestsSent != 2 {
		t.Fatalf("requests sent = %d, want 2", res.RequestsSent)
	}
	if got := len(f.onDemandRequestTimes); got != 2 {
		t.Fatalf("request times = %d, want 2", got)
	}
	if elapsed := f.onDemandRequestTimes[1].Sub(f.onDemandRequestTimes[0]); elapsed < delay {
		t.Fatalf("request spacing = %s, want at least %s", elapsed, delay)
	}
}

func TestBackfillHistoryDoesNotCountRejectedRequest(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	chatStr := chat.String()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chatStr, "dm", "Test Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(chatStr, "m1", base, "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	f.onDemandErr = fmt.Errorf("connection unavailable")

	var backfillErr error
	stderr := captureStderr(t, func() {
		_, backfillErr = a.BackfillHistory(context.Background(), BackfillOptions{
			ChatJID:        chatStr,
			Count:          50,
			Requests:       1,
			WaitPerRequest: time.Second,
			IdleExit:       200 * time.Millisecond,
		})
	})
	if backfillErr == nil || !strings.Contains(backfillErr.Error(), "connection unavailable") {
		t.Fatalf("BackfillHistory error = %v, want connection unavailable", backfillErr)
	}
	if strings.Contains(stderr, "Requesting ") {
		t.Fatalf("rejected request was reported as accepted: %q", stderr)
	}
}

func TestBackfillHistoryRequestDelayHonorsCancellation(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	chatStr := chat.String()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(chatStr, "dm", "Test Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(chatStr, "m2", base.Add(2*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	f.onDemandHistory = func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(chatStr),
				FromMe:    proto.Bool(false),
				ID:        proto.String("m1"),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Add(time.Second).Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:                       proto.String(chatStr),
				EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY.Enum(),
				Messages:                 []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := a.BackfillHistory(ctx, BackfillOptions{
		ChatJID:        chatStr,
		Count:          50,
		Requests:       2,
		WaitPerRequest: time.Second,
		RequestDelay:   time.Second,
		IdleExit:       200 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("BackfillHistory error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("cancellation took %s, want less than request delay", elapsed)
	}
	if got := len(f.onDemandRequestTimes); got != 1 {
		t.Fatalf("request times = %d, want 1 before cancellation", got)
	}
}

func TestNormalizeBackfillOptions(t *testing.T) {
	opts := normalizeBackfillOptions(BackfillOptions{})

	if opts.Count != DefaultBackfillCount {
		t.Fatalf("Count = %d, want %d", opts.Count, DefaultBackfillCount)
	}
	if opts.Requests != DefaultBackfillRequests {
		t.Fatalf("Requests = %d, want %d", opts.Requests, DefaultBackfillRequests)
	}
	if opts.WaitPerRequest <= 0 || opts.IdleExit <= 0 {
		t.Fatalf("durations must default positive: %+v", opts)
	}
}

func TestValidateBackfillOptionsCapsWork(t *testing.T) {
	err := validateBackfillOptions(BackfillOptions{
		Count:    MaxBackfillCount + 1,
		Requests: DefaultBackfillRequests,
	})
	if err == nil || !strings.Contains(err.Error(), "--count") {
		t.Fatalf("count error = %v", err)
	}

	err = validateBackfillOptions(BackfillOptions{
		Count:    DefaultBackfillCount,
		Requests: MaxBackfillRequests + 1,
	})
	if err == nil || !strings.Contains(err.Error(), "--requests") {
		t.Fatalf("requests error = %v", err)
	}
}

func configureBatchHistoryNotifications(
	f *fakeWA,
	build func(types.MessageInfo, int) *events.HistorySync,
) {
	payloads := make(map[*waE2E.HistorySyncNotification]*waHistorySync.HistorySync)
	f.onDemandEvent = func(lastKnown types.MessageInfo, count int) interface{} {
		history := build(lastKnown, count)
		if history == nil || history.Data == nil {
			return nil
		}
		syncType := waE2E.HistorySyncType_ON_DEMAND
		notif := &waE2E.HistorySyncNotification{SyncType: &syncType}
		payloads[notif] = history.Data
		return &events.Message{
			Message: &waProto.Message{
				ProtocolMessage: &waProto.ProtocolMessage{
					HistorySyncNotification: notif,
				},
			},
		}
	}
	f.downloadHistory = func(notif *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		history := payloads[notif]
		if history == nil {
			return nil, fmt.Errorf("missing synthetic history payload")
		}
		return history, nil
	}
}

func TestBackfillHistoryBatchUsesOneConnectionAndTracksEachChat(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	chats := []types.JID{
		{User: "123", Server: types.DefaultUserServer},
		{User: "456", Server: types.DefaultUserServer},
	}
	for _, chat := range chats {
		chatJID := chat.String()
		if err := a.db.UpsertChat(chatJID, "dm", "Synthetic Contact", base); err != nil {
			t.Fatalf("UpsertChat %s: %v", chatJID, err)
		}
		if err := a.db.UpsertMessage(storeUpsertMessage(chatJID, "current", base.Add(2*time.Second), "current")); err != nil {
			t.Fatalf("UpsertMessage %s: %v", chatJID, err)
		}
	}

	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		chatJID := lastKnown.Chat.String()
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(chatJID),
				FromMe:    proto.Bool(false),
				ID:        proto.String("older"),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Add(time.Second).Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:       proto.String(chatJID),
				Messages: []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := a.BackfillHistoryBatch(ctx, BackfillBatchOptions{
		ChatJIDs:     []string{chats[0].String(), chats[1].String()},
		Count:        50,
		BatchSize:    2,
		WaitPerBatch: time.Second,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	if f.connectCalls != 1 {
		t.Fatalf("connect calls = %d, want 1", f.connectCalls)
	}
	if len(f.onDemandRequestTimes) != 2 {
		t.Fatalf("request calls = %d, want 2", len(f.onDemandRequestTimes))
	}
	if len(res.Chats) != 2 {
		t.Fatalf("chat results = %d, want 2", len(res.Chats))
	}
	for _, got := range res.Chats {
		if got.RequestsSent != 1 || got.ResponsesSeen != 1 {
			t.Fatalf("%s requests/responses = %d/%d, want 1/1", got.ChatJID, got.RequestsSent, got.ResponsesSeen)
		}
		if got.MessagesReceived != 1 || got.MessagesAdded != 1 {
			t.Fatalf("%s received/added = %d/%d, want 1/1", got.ChatJID, got.MessagesReceived, got.MessagesAdded)
		}
		if got.Error != "" {
			t.Fatalf("%s error = %q, want empty", got.ChatJID, got.Error)
		}
	}
}

func TestBackfillHistoryBatchSupportsDirectLIDWithoutPNMapping(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	lid := types.JID{User: "987", Server: types.HiddenUserServer}
	if err := a.db.UpsertChat(lid.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(lid.String(), "current", base, "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID: proto.String(lid.String()),
			}},
		}}
	})

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{lid.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		LIDFallback:  true,
		Requests:     2,
		WaitPerBatch: time.Second,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 1 || got.ResponsesSeen != 1 ||
		got.RequestIdentity != "lid" || got.Error != "" {
		t.Fatalf("direct LID result = %+v, want one successful LID request", got)
	}
}

func TestBackfillHistoryBatchPreservesDirectLIDPreferenceAfterCanonicalizingToPN(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := types.JID{User: "123", Server: types.DefaultUserServer}
	lid := types.JID{User: "987", Server: types.HiddenUserServer}
	f.lids[lid] = pn
	if err := a.db.UpsertChat(pn.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(pn.String(), "current", base, "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		if lastKnown.Chat.Server != types.HiddenUserServer {
			t.Fatalf("request chat = %s, want direct LID identity", lastKnown.Chat)
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID: proto.String(lid.String()),
			}},
		}}
	})

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{lid.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		LIDFallback:  true,
		Requests:     2,
		WaitPerBatch: time.Second,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.ChatJID != pn.String() || got.RequestsSent != 1 ||
		got.ResponsesSeen != 1 || got.RequestIdentity != "lid" || got.Error != "" {
		t.Fatalf("canonical direct LID result = %+v, want one successful LID request for PN chat", got)
	}
}

func TestBackfillHistoryBatchRecordsPerChatTimeout(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	responsive := types.JID{User: "123", Server: types.DefaultUserServer}
	silent := types.JID{User: "456", Server: types.DefaultUserServer}
	for _, chat := range []types.JID{responsive, silent} {
		chatJID := chat.String()
		if err := a.db.UpsertChat(chatJID, "dm", "Synthetic Contact", base); err != nil {
			t.Fatalf("UpsertChat %s: %v", chatJID, err)
		}
		if err := a.db.UpsertMessage(storeUpsertMessage(chatJID, "current", base, "current")); err != nil {
			t.Fatalf("UpsertMessage %s: %v", chatJID, err)
		}
	}
	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		if lastKnown.Chat.String() == silent.String() {
			return nil
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID: proto.String(responsive.String()),
			}},
		}}
	})

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{silent.String(), responsive.String()},
		Count:        50,
		BatchSize:    2,
		MaxInFlight:  2,
		WaitPerBatch: 20 * time.Millisecond,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	if res.Chats[1].ResponsesSeen != 1 || res.Chats[1].Error != "" {
		t.Fatalf("responsive result = %+v, want one response and no error", res.Chats[1])
	}
	if res.Chats[0].RequestsSent != 1 || res.Chats[0].ResponsesSeen != 0 {
		t.Fatalf("silent requests/responses = %d/%d, want 1/0", res.Chats[0].RequestsSent, res.Chats[0].ResponsesSeen)
	}
	if !strings.Contains(res.Chats[0].Error, "timed out") {
		t.Fatalf("silent error = %q, want timeout", res.Chats[0].Error)
	}
}

func TestBackfillHistoryBatchUsesLongerBackoffAfterNoResponseBatch(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	chats := []types.JID{
		{User: "123", Server: types.DefaultUserServer},
		{User: "456", Server: types.DefaultUserServer},
	}
	for _, chat := range chats {
		if err := a.db.UpsertChat(chat.String(), "dm", "Synthetic Contact", base); err != nil {
			t.Fatalf("UpsertChat: %v", err)
		}
		if err := a.db.UpsertMessage(storeUpsertMessage(chat.String(), "current", base, "current")); err != nil {
			t.Fatalf("UpsertMessage: %v", err)
		}
	}

	_, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:       []string{chats[0].String(), chats[1].String()},
		Count:          50,
		BatchSize:      1,
		MaxInFlight:    1,
		Requests:       1,
		WaitPerBatch:   5 * time.Millisecond,
		BatchDelay:     time.Millisecond,
		TimeoutBackoff: 25 * time.Millisecond,
		IdleExit:       5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	if len(f.onDemandRequestTimes) != 2 {
		t.Fatalf("request calls = %d, want 2", len(f.onDemandRequestTimes))
	}
	if gap := f.onDemandRequestTimes[1].Sub(f.onDemandRequestTimes[0]); gap < 25*time.Millisecond {
		t.Fatalf("request gap = %s, want no-response backoff of at least 25ms", gap)
	}
}

func TestBackfillHistoryBatchFallbackRespectsRequestCap(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := types.JID{User: "123", Server: types.DefaultUserServer}
	lid := types.JID{User: "987", Server: types.HiddenUserServer}
	f.lids[lid] = pn
	if err := a.db.UpsertChat(pn.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(pn.String(), "current", base, "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{pn.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		LIDFallback:  true,
		Requests:     1,
		WaitPerBatch: 20 * time.Millisecond,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 1 || got.RequestIdentity != "pn" {
		t.Fatalf("capped result = %+v, want one PN request", got)
	}
	if len(f.onDemandRequestTimes) != 1 {
		t.Fatalf("request calls = %d, want 1", len(f.onDemandRequestTimes))
	}
}

func TestBackfillHistoryBatchFallsBackFromPNToLID(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := types.JID{User: "123", Server: types.DefaultUserServer}
	lid := types.JID{User: "987", Server: types.HiddenUserServer}
	f.lids[lid] = pn
	if err := a.db.UpsertChat(pn.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(pn.String(), "current", base.Add(2*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		if lastKnown.Chat.Server != types.HiddenUserServer {
			return nil
		}
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(lid.String()),
				FromMe:    proto.Bool(false),
				ID:        proto.String("older"),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:                       proto.String(lid.String()),
				EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum(),
				Messages:                 []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}}
	})

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{pn.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		LIDFallback:  true,
		Requests:     2,
		WaitPerBatch: 20 * time.Millisecond,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 2 || got.ResponsesSeen != 1 {
		t.Fatalf("requests/responses = %d/%d, want 2/1", got.RequestsSent, got.ResponsesSeen)
	}
	if got.RequestIdentity != "lid" || got.MessagesAdded != 1 || got.Error != "" {
		t.Fatalf("fallback result = %+v, want successful LID response with one added message", got)
	}
	preferred, err := a.db.GetHistoryRequestIdentity(pn.String())
	if err != nil {
		t.Fatalf("GetHistoryRequestIdentity: %v", err)
	}
	if preferred != "lid" {
		t.Fatalf("preferred identity = %q, want lid", preferred)
	}

	requestsBefore := len(f.onDemandRequestTimes)
	res, err = a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{pn.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		LIDFallback:  true,
		Requests:     2,
		WaitPerBatch: 20 * time.Millisecond,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("second BackfillHistoryBatch: %v", err)
	}
	got = res.Chats[0]
	if got.RequestIdentity != "lid" || got.ResponsesSeen != 1 || got.Error != "" {
		t.Fatalf("preferred LID result = %+v, want immediate LID success", got)
	}
	if addedRequests := len(f.onDemandRequestTimes) - requestsBefore; addedRequests != 1 {
		t.Fatalf("second-run requests = %d, want 1 preferred-identity request", addedRequests)
	}
}

func TestBackfillHistoryBatchFallsBackAfterEmptyPNResponse(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := types.JID{User: "123", Server: types.DefaultUserServer}
	lid := types.JID{User: "987", Server: types.HiddenUserServer}
	f.lids[lid] = pn
	if err := a.db.UpsertChat(pn.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(pn.String(), "current", base.Add(2*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		conversation := &waHistorySync.Conversation{
			ID: proto.String(lastKnown.Chat.String()),
		}
		if lastKnown.Chat.Server == types.HiddenUserServer {
			conversation.EndOfHistoryTransferType =
				waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum()
			conversation.Messages = []*waHistorySync.HistorySyncMsg{{
				Message: &waWeb.WebMessageInfo{
					Key: &waCommon.MessageKey{
						RemoteJID: proto.String(lid.String()),
						FromMe:    proto.Bool(false),
						ID:        proto.String("older"),
					},
					MessageTimestamp: proto.Uint64(uint64(base.Unix())),
					Message:          &waProto.Message{Conversation: proto.String("older")},
				},
			}}
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType:      waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{conversation},
		}}
	})

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{pn.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		LIDFallback:  true,
		Requests:     2,
		WaitPerBatch: time.Second,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 2 || got.ResponsesSeen != 2 ||
		got.MessagesAdded != 1 || got.RequestIdentity != "lid" || got.Error != "" {
		t.Fatalf("empty-PN fallback result = %+v, want useful LID fallback", got)
	}
	preferred, err := a.db.GetHistoryRequestIdentity(pn.String())
	if err != nil {
		t.Fatalf("GetHistoryRequestIdentity: %v", err)
	}
	if preferred != "lid" {
		t.Fatalf("preferred identity = %q, want useful LID route", preferred)
	}
}

func TestBackfillHistoryBatchCorrelatesLateResponseToOriginalRequest(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := types.JID{User: "123", Server: types.DefaultUserServer}
	lid := types.JID{User: "987", Server: types.HiddenUserServer}
	f.lids[lid] = pn
	if err := a.db.UpsertChat(pn.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(pn.String(), "current", base.Add(3*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	var payloadMu sync.Mutex
	payloads := make(map[*waE2E.HistorySyncNotification]*waHistorySync.HistorySync)
	f.downloadHistory = func(notif *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		payloadMu.Lock()
		defer payloadMu.Unlock()
		return payloads[notif], nil
	}
	f.onDemandEvent = func(lastKnown types.MessageInfo, count int) interface{} {
		syncType := waE2E.HistorySyncType_ON_DEMAND
		notif := &waE2E.HistorySyncNotification{
			SyncType:          &syncType,
			OriginalMessageID: proto.String(""),
		}
		delay := 5 * time.Millisecond
		messageID := "lid-older"
		remoteJID := lid.String()
		if lastKnown.Chat.Server == types.DefaultUserServer {
			delay = 35 * time.Millisecond
			messageID = "late-pn-older"
			remoteJID = pn.String()
		}
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(remoteJID),
				FromMe:    proto.Bool(false),
				ID:        proto.String(messageID),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		endType := waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY
		history := &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:                       proto.String(remoteJID),
				EndOfHistoryTransferType: &endType,
				Messages:                 []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}
		payloadMu.Lock()
		payloads[notif] = history
		payloadMu.Unlock()
		event := &events.Message{
			Message: &waProto.Message{
				ProtocolMessage: &waProto.ProtocolMessage{
					HistorySyncNotification: notif,
				},
			},
		}
		go func() {
			time.Sleep(delay)
			f.emit(event)
		}()
		return nil
	}

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{pn.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		LIDFallback:  true,
		Requests:     2,
		WaitPerBatch: 20 * time.Millisecond,
		IdleExit:     50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 2 || got.ResponsesSeen != 1 || got.RequestIdentity != "lid" {
		t.Fatalf("correlated result = %+v, want one LID-correlated response after two requests", got)
	}
}

func TestBackfillHistoryBatchRejectsLateResponseIDForNewSameIdentityRequest(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := types.JID{User: "123", Server: types.DefaultUserServer}
	if err := a.db.UpsertChat(pn.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(pn.String(), "current", base.Add(2*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	var payloadMu sync.Mutex
	payloads := make(map[*waE2E.HistorySyncNotification]*waHistorySync.HistorySync)
	f.downloadHistory = func(notif *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		payloadMu.Lock()
		defer payloadMu.Unlock()
		return payloads[notif], nil
	}
	var calls int
	f.onDemandEvent = func(lastKnown types.MessageInfo, count int) interface{} {
		calls++
		if calls > 1 {
			return nil
		}
		syncType := waE2E.HistorySyncType_ON_DEMAND
		requestID := "req-1"
		notif := &waE2E.HistorySyncNotification{
			SyncType:          &syncType,
			OriginalMessageID: &requestID,
		}
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(pn.String()),
				FromMe:    proto.Bool(false),
				ID:        proto.String("older"),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		history := &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:                       proto.String(pn.String()),
				EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY.Enum(),
				Messages:                 []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}
		payloadMu.Lock()
		payloads[notif] = history
		payloadMu.Unlock()
		event := &events.Message{
			Message: &waProto.Message{
				ProtocolMessage: &waProto.ProtocolMessage{
					HistorySyncNotification: notif,
				},
			},
		}
		go func() {
			time.Sleep(5 * time.Millisecond)
			f.emit(event)
		}()
		return event
	}

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{pn.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		Requests:     2,
		WaitPerBatch: 20 * time.Millisecond,
		IdleExit:     30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 2 || got.ResponsesSeen != 1 || got.MessagesAdded != 1 {
		t.Fatalf("late same-identity result = %+v, want two requests but only the first response", got)
	}
	if got.Error != "" {
		t.Fatalf("late optional retry error = %q, want earlier success preserved", got.Error)
	}
}

func TestBackfillHistoryBatchReconcilesIDLessPreRegistrationResponse(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := types.JID{User: "123", Server: types.DefaultUserServer}
	if err := a.db.UpsertChat(pn.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(pn.String(), "current", base, "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	payloads := make(map[*waE2E.HistorySyncNotification]*waHistorySync.HistorySync)
	f.onDemandEvent = func(lastKnown types.MessageInfo, count int) interface{} {
		syncType := waE2E.HistorySyncType_ON_DEMAND
		emptyRequestID := ""
		notif := &waE2E.HistorySyncNotification{
			SyncType:          &syncType,
			OriginalMessageID: &emptyRequestID,
		}
		payloads[notif] = &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID: proto.String(pn.String()),
			}},
		}
		return &events.Message{
			Message: &waProto.Message{
				ProtocolMessage: &waProto.ProtocolMessage{
					HistorySyncNotification: notif,
				},
			},
		}
	}
	f.downloadHistory = func(notif *waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		return payloads[notif], nil
	}

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{pn.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		Requests:     1,
		WaitPerBatch: time.Second,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 1 || got.ResponsesSeen != 1 || got.Error != "" {
		t.Fatalf("ID-less early response result = %+v, want one correlated response", got)
	}
}

func TestBackfillHistoryBatchRepeatsOnlyWhileRowsGrow(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	chat := types.JID{User: "123", Server: types.DefaultUserServer}
	if err := a.db.UpsertChat(chat.String(), "dm", "Synthetic Contact", base); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}
	if err := a.db.UpsertMessage(storeUpsertMessage(chat.String(), "current", base.Add(2*time.Second), "current")); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	configureBatchHistoryNotifications(f, func(lastKnown types.MessageInfo, count int) *events.HistorySync {
		older := &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(chat.String()),
				FromMe:    proto.Bool(false),
				ID:        proto.String("older"),
			},
			MessageTimestamp: proto.Uint64(uint64(base.Unix())),
			Message:          &waProto.Message{Conversation: proto.String("older")},
		}
		return &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
			Conversations: []*waHistorySync.Conversation{{
				ID:                       proto.String(chat.String()),
				EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY.Enum(),
				Messages:                 []*waHistorySync.HistorySyncMsg{{Message: older}},
			}},
		}}
	})

	res, err := a.BackfillHistoryBatch(context.Background(), BackfillBatchOptions{
		ChatJIDs:     []string{chat.String()},
		Count:        50,
		BatchSize:    1,
		MaxInFlight:  1,
		Requests:     10,
		WaitPerBatch: time.Second,
		IdleExit:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("BackfillHistoryBatch: %v", err)
	}
	got := res.Chats[0]
	if got.RequestsSent != 2 || got.ResponsesSeen != 2 {
		t.Fatalf("requests/responses = %d/%d, want 2/2", got.RequestsSent, got.ResponsesSeen)
	}
	if got.MessagesAdded != 1 {
		t.Fatalf("messages added = %d, want 1", got.MessagesAdded)
	}
}

func storeUpsertMessage(chatJID, id string, ts time.Time, text string) store.UpsertMessageParams {
	return store.UpsertMessageParams{
		ChatJID:    chatJID,
		MsgID:      id,
		SenderJID:  chatJID,
		SenderName: "Alice",
		Timestamp:  ts,
		FromMe:     false,
		Text:       text,
	}
}
