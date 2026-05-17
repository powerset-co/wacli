package main

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/wacli/internal/fsutil"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
)

func TestParsePlatformType(t *testing.T) {
	if got := parsePlatformType("desktop"); got != waCompanionReg.DeviceProps_DESKTOP {
		t.Fatalf("desktop parsed as %v", got)
	}
	if got := parsePlatformType("bogus"); got != waCompanionReg.DeviceProps_CHROME {
		t.Fatalf("bogus parsed as %v", got)
	}
}

func TestDetectDeviceLabel(t *testing.T) {
	host := func() (string, error) { return "workstation", nil }
	readFile := func(string) ([]byte, error) { return []byte(`PRETTY_NAME="Ubuntu 24.04 LTS"`), nil }

	if got := detectDeviceLabel("linux", host, readFile); got != "wacli - Ubuntu 24.04 LTS (workstation)" {
		t.Fatalf("detectDeviceLabel = %q", got)
	}
}

func TestDetectDeviceLabelFallbacks(t *testing.T) {
	noHost := func() (string, error) { return "", errors.New("no hostname") }
	noFile := func(string) ([]byte, error) { return nil, errors.New("missing") }

	if got := detectDeviceLabel("darwin", noHost, noFile); got != "wacli - macOS" {
		t.Fatalf("darwin label = %q", got)
	}
	if got := detectDeviceLabel("", noHost, noFile); got != "wacli" {
		t.Fatalf("empty label = %q", got)
	}
}

func TestSanitizeReplacesTerminalControls(t *testing.T) {
	got := sanitize("Alice\x1b[31m\nBob\x7f")
	if got != "Alice Bob" {
		t.Fatalf("sanitize = %q", got)
	}
}

func TestSanitizeBodyPreservesMessageLayout(t *testing.T) {
	got := sanitizeBody("one\n\ttwo\x1b[31m\rthree\x7f")
	if got != "one\n\ttwothree" {
		t.Fatalf("sanitizeBody = %q", got)
	}
}

func TestReadRegularFileLimitedRejectsNonRegularAndOversized(t *testing.T) {
	dir := t.TempDir()
	if _, err := readRegularFileLimited(dir, 10); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("expected non-regular error, got %v", err)
	}

	path := filepath.Join(t.TempDir(), "input.bin")
	if err := fsutil.WritePrivateFile(path, []byte("hello")); err != nil {
		t.Fatalf("WritePrivateFile: %v", err)
	}
	if _, err := readRegularFileLimited(path, 4); err == nil || !strings.Contains(err.Error(), "file too large") {
		t.Fatalf("expected file too large error, got %v", err)
	}
	got, err := readRegularFileLimited(path, 5)
	if err != nil {
		t.Fatalf("readRegularFileLimited: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("data = %q", string(got))
	}
}
