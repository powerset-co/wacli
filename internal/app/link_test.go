package app

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func TestLinkReturnsAtFirstLoginWithoutStoringHistory(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	f.connectEvents = []interface{}{&events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: types.JID{User: "15550100", Server: types.DefaultUserServer}},
		ID:            "m-history",
		Timestamp:     time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
	}}}
	a.wa = f

	if err := a.Link(context.Background(), SyncOptions{AllowQR: true}); err != nil {
		t.Fatalf("Link: %v", err)
	}
	if f.connectCalls != 1 {
		t.Fatalf("connect calls = %d, want 1", f.connectCalls)
	}
	if n, err := a.db.CountMessages(); err != nil || n != 0 {
		t.Fatalf("messages stored = %d (%v), want 0", n, err)
	}
}
