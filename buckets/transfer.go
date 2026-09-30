package buckets

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/errors"
)

// TransferResult holds the result of uploading a single chunk
type TransferResult struct {
	ETag string
}

// Transfer uploads data to the given URL and returns the ETag
func Transfer(ctx context.Context, cfg *config.Config, uploadURL string, r io.Reader, size int64) (*TransferResult, error) {
	body := newTransferBody(r)
	req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create transfer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size

	defer body.wait()

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute transfer request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.NewHTTPError(resp, "transfer")
	}

	// Extract ETag from response header
	etag := resp.Header.Get("ETag")
	// Strip quotes if present
	etag = strings.Trim(etag, "\"")

	return &TransferResult{ETag: etag}, nil
}

// transferBody wraps a request body without closing the caller's reader and
// signals when the transport is done with it.
type transferBody struct {
	io.Reader
	once   sync.Once
	closed chan struct{}
}

func newTransferBody(r io.Reader) *transferBody {
	return &transferBody{Reader: r, closed: make(chan struct{})}
}

func (b *transferBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func (b *transferBody) wait() {
	<-b.closed
}
