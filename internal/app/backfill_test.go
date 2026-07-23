package app

import (
	"context"
	"fmt"
	"strings"
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

	f.onDemandHistory = func(lastKnown types.MessageInfo, count int) *events.HistorySync {
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
