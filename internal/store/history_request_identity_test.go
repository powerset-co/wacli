package store

import (
	"strings"
	"testing"
	"time"
)

func TestHistoryRequestIdentityRoundTrip(t *testing.T) {
	db := openTestDB(t)
	chatJID := "15550001111@s.whatsapp.net"
	if err := db.UpsertChat(chatJID, "dm", "Synthetic Contact", time.Now()); err != nil {
		t.Fatalf("UpsertChat: %v", err)
	}

	got, err := db.GetHistoryRequestIdentity(chatJID)
	if err != nil {
		t.Fatalf("GetHistoryRequestIdentity empty: %v", err)
	}
	if got != "" {
		t.Fatalf("initial identity = %q, want empty", got)
	}

	if err := db.SetHistoryRequestIdentity(chatJID, HistoryRequestIdentityLID); err != nil {
		t.Fatalf("SetHistoryRequestIdentity lid: %v", err)
	}
	got, err = db.GetHistoryRequestIdentity(chatJID)
	if err != nil {
		t.Fatalf("GetHistoryRequestIdentity lid: %v", err)
	}
	if got != HistoryRequestIdentityLID {
		t.Fatalf("identity = %q, want lid", got)
	}

	if err := db.SetHistoryRequestIdentity(chatJID, HistoryRequestIdentityPN); err != nil {
		t.Fatalf("SetHistoryRequestIdentity pn: %v", err)
	}
	got, err = db.GetHistoryRequestIdentity(chatJID)
	if err != nil {
		t.Fatalf("GetHistoryRequestIdentity pn: %v", err)
	}
	if got != HistoryRequestIdentityPN {
		t.Fatalf("identity = %q, want pn", got)
	}

	if err := db.UpsertChat(chatJID, "dm", "Synthetic Contact Updated", time.Now()); err != nil {
		t.Fatalf("UpsertChat after preference: %v", err)
	}
	got, err = db.GetHistoryRequestIdentity(chatJID)
	if err != nil {
		t.Fatalf("GetHistoryRequestIdentity after UpsertChat: %v", err)
	}
	if got != HistoryRequestIdentityPN {
		t.Fatalf("identity after UpsertChat = %q, want pn", got)
	}
}

func TestHistoryRequestIdentityRejectsInvalidValue(t *testing.T) {
	db := openTestDB(t)
	err := db.SetHistoryRequestIdentity("15550001111@s.whatsapp.net", "unknown")
	if err == nil || !strings.Contains(err.Error(), "pn or lid") {
		t.Fatalf("invalid identity error = %v", err)
	}
}
