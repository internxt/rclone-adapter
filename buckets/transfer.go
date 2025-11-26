package buckets

import (
	"fmt"
	"io"
	"net/http"

	"github.com/internxt/rclone-adapter/config"
)

func Transfer(cfg *config.Config, part UploadPart, r io.Reader, size int64) error {
	uploadURL := ""
	if len(part.URLs) > part.Index {
		uploadURL = part.URLs[part.Index]
	} else if part.URL != "" {
		uploadURL = part.URL
	} else {
		return fmt.Errorf("no upload URL provided for part index %d", part.Index)
	}

	req, err := http.NewRequest("PUT", uploadURL, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size
	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("transfer failed: status %d, %s", resp.StatusCode, string(body))
	}
	return nil
}
