package wa

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/openclaw/wacli/internal/fsutil"
	"go.mau.fi/whatsmeow"
)

const MaxMediaDownloadSize = 100 * 1024 * 1024

// WhatsApp writes encrypted media as padded ciphertext plus a 10-byte MAC before
// truncating and decrypting it in place.
const maxEncryptedMediaDownloadOverhead = 16 + 10

func MediaTypeFromString(mediaType string) (whatsmeow.MediaType, error) {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "image":
		return whatsmeow.MediaImage, nil
	case "video":
		return whatsmeow.MediaVideo, nil
	case "audio":
		return whatsmeow.MediaAudio, nil
	case "document":
		return whatsmeow.MediaDocument, nil
	case "sticker":
		return whatsmeow.MediaImage, nil
	default:
		return "", fmt.Errorf("unsupported media type: %s", mediaType)
	}
}

func (c *Client) DownloadMediaToFile(ctx context.Context, directPath string, encFileHash, fileHash, mediaKey []byte, fileLength uint64, mediaType, mmsType string, targetPath string) (int64, error) {
	c.mu.Lock()
	cli := c.client
	c.mu.Unlock()
	if cli == nil || !cli.IsConnected() {
		return 0, fmt.Errorf("not connected")
	}
	if strings.TrimSpace(directPath) == "" {
		return 0, fmt.Errorf("direct path is required")
	}
	mt, err := MediaTypeFromString(mediaType)
	if err != nil {
		return 0, err
	}

	if err := fsutil.EnsureWritableDir(filepath.Dir(targetPath)); err != nil {
		return 0, fmt.Errorf("create output dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(targetPath), ".wacli-download-*")
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	success := false
	defer func() {
		_ = tmpFile.Close()
		if !success {
			_ = os.Remove(tmpName)
		}
	}()

	length, err := mediaDownloadLength(fileLength)
	if err != nil {
		return 0, err
	}

	limitedFile := &limitedDownloadFile{File: tmpFile, max: MaxMediaDownloadSize + maxEncryptedMediaDownloadOverhead, userMax: MaxMediaDownloadSize}
	if err := cli.DownloadMediaWithPathToFile(ctx, directPath, encFileHash, fileHash, mediaKey, length, mt, mmsType, limitedFile); err != nil {
		return 0, err
	}
	info, err := tmpFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat temp media file: %w", err)
	}
	if info.Size() > MaxMediaDownloadSize {
		return 0, fmt.Errorf("media too large; maximum download size is %d bytes", MaxMediaDownloadSize)
	}
	if err := tmpFile.Sync(); err != nil {
		return 0, fmt.Errorf("flush temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return 0, fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		return 0, fmt.Errorf("move media file: %w", err)
	}
	success = true

	info, err = os.Stat(targetPath)
	if err != nil {
		return 0, fmt.Errorf("stat output file: %w", err)
	}
	return info.Size(), nil
}

type limitedDownloadFile struct {
	*os.File
	max     int64
	userMax int64
	written int64
}

func (f *limitedDownloadFile) Write(p []byte) (int, error) {
	off, err := f.File.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if off+int64(len(p)) > f.max {
		return 0, f.limitError()
	}
	n, err := f.File.Write(p)
	f.noteWritten(off + int64(n))
	return n, err
}

func (f *limitedDownloadFile) WriteAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > f.max {
		return 0, f.limitError()
	}
	n, err := f.File.WriteAt(p, off)
	f.noteWritten(off + int64(n))
	return n, err
}

func (f *limitedDownloadFile) ReadFrom(r io.Reader) (int64, error) {
	off, err := f.File.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	remaining := f.max - off
	if remaining < 0 {
		remaining = 0
	}
	n, err := io.Copy(f.File, io.LimitReader(r, remaining))
	f.noteWritten(off + n)
	if err != nil {
		return n, err
	}
	var probe [1]byte
	extra, err := r.Read(probe[:])
	if extra > 0 {
		return n, f.limitError()
	}
	if err != nil && err != io.EOF {
		return n, err
	}
	return n, nil
}

func (f *limitedDownloadFile) Truncate(size int64) error {
	if size > f.max {
		return f.limitError()
	}
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	if f.written > size {
		f.written = size
	}
	return nil
}

func (f *limitedDownloadFile) limitError() error {
	max := f.userMax
	if max <= 0 {
		max = f.max
	}
	return fmt.Errorf("media too large; maximum download size is %d bytes", max)
}

func (f *limitedDownloadFile) noteWritten(end int64) {
	if end > f.written {
		f.written = end
	}
}

func mediaDownloadLength(fileLength uint64) (int, error) {
	if fileLength > MaxMediaDownloadSize {
		return 0, fmt.Errorf("media too large (%d bytes); maximum download size is %d bytes", fileLength, MaxMediaDownloadSize)
	}
	if fileLength > 0 {
		return int(fileLength), nil
	}
	return -1, nil
}
