package wa

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMediaTypeFromString(t *testing.T) {
	for _, tc := range []string{"image", "video", "audio", "document", "sticker"} {
		if _, err := MediaTypeFromString(tc); err != nil {
			t.Fatalf("expected %s to be supported: %v", tc, err)
		}
	}
	if _, err := MediaTypeFromString("nope"); err == nil {
		t.Fatalf("expected error for unsupported type")
	}
}

func TestMediaDownloadLengthRejectsOversizedMedia(t *testing.T) {
	_, err := mediaDownloadLength(MaxMediaDownloadSize + 1)
	if err == nil || !strings.Contains(err.Error(), "media too large") {
		t.Fatalf("expected media too large error, got %v", err)
	}
}

func TestMediaDownloadLength(t *testing.T) {
	if got, err := mediaDownloadLength(0); err != nil || got != -1 {
		t.Fatalf("length(0) = %d, %v; want -1, nil", got, err)
	}
	if got, err := mediaDownloadLength(123); err != nil || got != 123 {
		t.Fatalf("length(123) = %d, %v; want 123, nil", got, err)
	}
}

func TestLimitedDownloadFileRejectsWritesPastLimit(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "download.bin"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()

	limited := &limitedDownloadFile{File: f, max: 5}
	if n, err := limited.Write([]byte("hello")); err != nil || n != 5 {
		t.Fatalf("Write = %d, %v; want 5, nil", n, err)
	}
	if _, err := limited.Write([]byte("!")); err == nil || !strings.Contains(err.Error(), "media too large") {
		t.Fatalf("expected media too large error, got %v", err)
	}
	if _, err := limited.WriteAt([]byte("x"), 5); err == nil || !strings.Contains(err.Error(), "media too large") {
		t.Fatalf("expected WriteAt media too large error, got %v", err)
	}
	if _, err := limited.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if n, err := limited.Write([]byte("hey")); err != nil || n != 3 {
		t.Fatalf("retry Write = %d, %v; want 3, nil", n, err)
	}
	if err := limited.Truncate(2); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := limited.WriteAt([]byte("!"), 4); err != nil {
		t.Fatalf("WriteAt after truncate: %v", err)
	}
}

func TestLimitedDownloadFileReadFromEnforcesLimit(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "download.bin"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()

	limited := &limitedDownloadFile{File: f, max: 5}
	n, err := limited.ReadFrom(bytes.NewReader([]byte("hello!")))
	if err == nil || !strings.Contains(err.Error(), "media too large") {
		t.Fatalf("ReadFrom = %d, %v; want media too large error", n, err)
	}
	if n != 5 {
		t.Fatalf("ReadFrom bytes = %d, want 5", n)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 5 {
		t.Fatalf("file size = %d, want 5", info.Size())
	}
}

func TestLimitedDownloadFileAllowsEncryptedOverheadBeforeTruncate(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "download.bin"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()

	limited := &limitedDownloadFile{File: f, max: 5 + maxEncryptedMediaDownloadOverhead, userMax: 5}
	if n, err := limited.Write(bytes.Repeat([]byte("x"), 5+maxEncryptedMediaDownloadOverhead)); err != nil || n != 5+maxEncryptedMediaDownloadOverhead {
		t.Fatalf("Write encrypted bytes = %d, %v; want overhead accepted", n, err)
	}
	if err := limited.Truncate(5); err != nil {
		t.Fatalf("Truncate to plaintext size: %v", err)
	}
	if _, err := limited.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if _, err := limited.Write(bytes.Repeat([]byte("x"), 5+maxEncryptedMediaDownloadOverhead+1)); err == nil || !strings.Contains(err.Error(), "maximum download size is 5 bytes") {
		t.Fatalf("expected user-facing media limit error, got %v", err)
	}
}
