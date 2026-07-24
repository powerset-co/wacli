package store

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	HistoryRequestIdentityPN  = "pn"
	HistoryRequestIdentityLID = "lid"
)

func (d *DB) GetHistoryRequestIdentity(chatJID string) (string, error) {
	chatJID = strings.TrimSpace(chatJID)
	if chatJID == "" {
		return "", fmt.Errorf("chat JID is required")
	}
	var identity string
	err := d.sql.QueryRow(
		`SELECT COALESCE(history_request_identity, '') FROM chats WHERE jid = ?`,
		chatJID,
	).Scan(&identity)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	switch identity {
	case HistoryRequestIdentityPN, HistoryRequestIdentityLID:
		return identity, nil
	default:
		return "", nil
	}
}

func (d *DB) SetHistoryRequestIdentity(chatJID, identity string) error {
	chatJID = strings.TrimSpace(chatJID)
	if chatJID == "" {
		return fmt.Errorf("chat JID is required")
	}
	identity = strings.TrimSpace(strings.ToLower(identity))
	if identity != HistoryRequestIdentityPN && identity != HistoryRequestIdentityLID {
		return fmt.Errorf("history request identity must be pn or lid")
	}
	result, err := d.sql.Exec(
		`UPDATE chats SET history_request_identity = ? WHERE jid = ?`,
		identity,
		chatJID,
	)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated == 0 {
		return fmt.Errorf("chat %s not found", chatJID)
	}
	return nil
}
