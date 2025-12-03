package buckets

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/internxt/rclone-adapter/config"
)

// TransferResult holds the result of uploading a single chunk
type TransferResult struct {
	ETag string
}

// Transfer uploads data to the given URL and returns the ETag
func Transfer(ctx context.Context, cfg *config.Config, uploadURL string, r io.Reader, size int64) (*TransferResult, error) {
	req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("transfer failed: status %d, %s", resp.StatusCode, string(body))
	}

	// Extract ETag from response header
	etag := resp.Header.Get("ETag")
	// Strip quotes if present
	etag = strings.Trim(etag, "\"")


	return &TransferResult{ETag: etag}, nil
}
