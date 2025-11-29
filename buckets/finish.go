package buckets

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/internxt/rclone-adapter/config"
)

type Shard struct {
	Hash string `json:"hash"`
	UUID string `json:"uuid"`
}

// CompletedPart represents a single uploaded part for multipart completion
type CompletedPart struct {
	PartNumber int    `json:"PartNumber"`
	ETag       string `json:"ETag"`
}

// MultipartShard represents a shard with multipart upload metadata
type MultipartShard struct {
	UUID     string          `json:"uuid"`
	Hash     string          `json:"hash"`
	UploadId string          `json:"UploadId"`
	Parts    []CompletedPart `json:"parts"`
}

type FinishUploadResp struct {
	Bucket   string `json:"bucket"`
	Index    string `json:"index"`
	ID       string `json:"id"`
	Version  int    `json:"version"`
	Created  string `json:"created"`
	Renewal  string `json:"renewal"`
	Mimetype string `json:"mimetype"`
	Filename string `json:"filename"`
}

func FinishUpload(cfg *config.Config, bucketID, index string, shards []Shard) (*FinishUploadResp, error) {
	url := cfg.Endpoints.Network().FinishUpload(bucketID)
	payload := map[string]interface{}{
		"index":  index,
		"shards": shards,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", cfg.BasicAuthHeader)
	req.Header.Set("internxt-version", "1.0")
	req.Header.Set("internxt-client", "drive-web")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == 500 && strings.Contains(bodyStr, "duplicate key error") {
			return nil, fmt.Errorf("file already exists on server (duplicate shard): %s", bodyStr)
		}
		return nil, fmt.Errorf("finish upload failed: status %d, %s", resp.StatusCode, bodyStr)
	}

	var result FinishUploadResp
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// FinishMultipartUpload completes a multipart upload session
func FinishMultipartUpload(cfg *config.Config, bucketID, index string, shard MultipartShard) (*FinishUploadResp, error) {
	url := cfg.Endpoints.Network().FinishUpload(bucketID)
	payload := map[string]any{
		"index":  index,
		"shards": []MultipartShard{shard},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", cfg.BasicAuthHeader)
	req.Header.Set("internxt-version", "1.0")
	req.Header.Set("internxt-client", "rclone") // TODO: define this ?
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == 500 && strings.Contains(bodyStr, "duplicate key error") {
			return nil, fmt.Errorf("file already exists on server (duplicate shard): %s", bodyStr)
		}
		return nil, fmt.Errorf("finish multipart upload failed: status %d, %s", resp.StatusCode, bodyStr)
	}

	var result FinishUploadResp
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
