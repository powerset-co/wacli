package app

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"golang.org/x/term"
)

var syncStatusTerminal = func() bool {
	return term.IsTerminal(int(os.Stderr.Fd()))
}

type syncStatus struct {
	mu            sync.Mutex
	w             io.Writer
	last          string
	lastLen       int
	conversations int
	messages      int64
}

func newSyncStatus(w io.Writer) *syncStatus {
	return &syncStatus{w: w}
}

func (a *App) beginSyncStatus() *syncStatus {
	if a == nil || a.eventsEnabled() || !syncStatusTerminal() {
		return nil
	}
	st := newSyncStatus(os.Stderr)
	a.statusMu.Lock()
	a.status = st
	a.statusMu.Unlock()
	return st
}

func (a *App) endSyncStatus(st *syncStatus) {
	if st == nil {
		return
	}
	st.Clear()
	a.statusMu.Lock()
	if a.status == st {
		a.status = nil
	}
	a.statusMu.Unlock()
}

func (a *App) currentSyncStatus() *syncStatus {
	if a == nil {
		return nil
	}
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	return a.status
}

func (s *syncStatus) Connected() {
	s.Set("Connected. Waiting for history sync...")
}

func (s *syncStatus) HistorySync(conversations int) {
	s.mu.Lock()
	s.conversations = conversations
	msg := s.historyMessageLocked()
	s.mu.Unlock()
	s.Set(msg)
}

func (s *syncStatus) Progress(messages int64) {
	s.mu.Lock()
	s.messages = messages
	msg := s.historyMessageLocked()
	s.mu.Unlock()
	s.Set(msg)
}

func (s *syncStatus) historyMessageLocked() string {
	if s.conversations > 0 {
		return fmt.Sprintf("Syncing history: %d conversations, %d messages stored", s.conversations, s.messages)
	}
	return fmt.Sprintf("Synced %d messages", s.messages)
}

func (s *syncStatus) Set(message string) {
	if s == nil || s.w == nil {
		return
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renderLocked(message)
}

func (s *syncStatus) PrintLine(message string) {
	if s == nil || s.w == nil {
		return
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	fmt.Fprintln(s.w, message)
}

func (s *syncStatus) WarnLine(message string) {
	if s == nil || s.w == nil {
		return
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	fmt.Fprintln(s.w, message)
	if s.last != "" {
		s.renderLocked(s.last)
	}
}

func (s *syncStatus) Clear() {
	if s == nil || s.w == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	s.last = ""
}

func (s *syncStatus) renderLocked(message string) {
	padding := ""
	if s.lastLen > len(message) {
		padding = strings.Repeat(" ", s.lastLen-len(message))
	}
	fmt.Fprintf(s.w, "\r%s%s", message, padding)
	s.last = message
	s.lastLen = len(message)
}

func (s *syncStatus) clearLocked() {
	if s.lastLen <= 0 {
		return
	}
	fmt.Fprintf(s.w, "\r%s\r", strings.Repeat(" ", s.lastLen))
	s.lastLen = 0
}
