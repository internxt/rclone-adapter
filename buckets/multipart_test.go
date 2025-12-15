package buckets

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/internxt/rclone-adapter/config"
)

// TestNewMultipartUploadState tests the initialization of multipart upload state
func TestNewMultipartUploadState(t *testing.T) {
	cfg := &config.Config{
		Mnemonic: TestMnemonic,
		Bucket:   TestBucket6,
	}

	testCases := []struct {
		name      string
		fileSize  int64
		wantParts int64
	}{
		{
			name:      "small file - 1 chunk",
			fileSize:  10 * 1024 * 1024, // 10 MB
			wantParts: 1,
		},
		{
			name:      "medium file - 4 chunks",
			fileSize:  100 * 1024 * 1024, // 100 MB
			wantParts: 4, // ceil(100 / 30)
		},
		{
			name:      "large file - 10 chunks",
			fileSize:  300 * 1024 * 1024, // 300 MB
			wantParts: 10,
		},
		{
			name:      "exact boundary - 2 chunks",
			fileSize:  60 * 1024 * 1024, // 60 MB
			wantParts: 2,
		},
		{
			name:      "just over boundary - 3 chunks",
			fileSize:  60*1024*1024 + 1, // 60 MB + 1 byte
			wantParts: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := newMultipartUploadState(cfg, tc.fileSize)
			if err != nil {
				t.Fatalf("newMultipartUploadState failed: %v", err)
			}

			if state.numParts != tc.wantParts {
				t.Errorf("expected %d parts, got %d", tc.wantParts, state.numParts)
			}

			if state.totalSize != tc.fileSize {
				t.Errorf("expected total size %d, got %d", tc.fileSize, state.totalSize)
			}

			if state.plainIndex == "" {
				t.Error("plainIndex should not be empty")
			}

			if state.encIndex == "" {
				t.Error("encIndex should not be empty")
			}

			if len(state.fileKey) != 32 {
				t.Errorf("expected file key length 32, got %d", len(state.fileKey))
			}

			if len(state.iv) != 16 {
				t.Errorf("expected IV length 16, got %d", len(state.iv))
			}

			if state.cipher == nil {
				t.Error("cipher should not be nil")
			}

			if state.maxConcurrency != config.DefaultMaxConcurrency {
				t.Errorf("expected max concurrency %d, got %d", config.DefaultMaxConcurrency, state.maxConcurrency)
			}
		})
	}
}

// TestEncryptedChunkPipeline tests the encryption pipeline
func TestEncryptedChunkPipeline(t *testing.T) {
	cfg := &config.Config{
		Mnemonic:   TestMnemonic,
		Bucket:     TestBucket6,
		HTTPClient: &http.Client{},
	}

	testData := bytes.Repeat([]byte("test data pattern "), 5*1024*1024) // ~90 MB
	fileSize := int64(len(testData))

	state, err := newMultipartUploadState(cfg, fileSize)
	if err != nil {
		t.Fatalf("newMultipartUploadState failed: %v", err)
	}

	var uploadCount atomic.Int32
	var receivedETags []string
	etagMutex := &sync.Mutex{}

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("expected PUT request, got %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		uploadedData, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read upload body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		hasher := sha1.New()
		hasher.Write(uploadedData)
		etag := hex.EncodeToString(hasher.Sum(nil))

		etagMutex.Lock()
		receivedETags = append(receivedETags, etag)
		etagMutex.Unlock()

		uploadCount.Add(1)

		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	urls := make([]string, state.numParts)
	for i := range urls {
		urls[i] = mockServer.URL
	}

	state.startResp = &StartUploadResp{
		Uploads: []UploadPart{
			{
				UUID:     "test-uuid",
				URLs:     urls,
				UploadId: "test-upload-id",
			},
		},
	}
	state.uuid = "test-uuid"
	state.uploadId = "test-upload-id"

	reader := bytes.NewReader(testData)
	parts, overallHash, err := state.encryptAndUploadPipelined(context.Background(), reader)
	if err != nil {
		t.Fatalf("encryptAndUploadPipelined failed: %v", err)
	}

	if int64(len(parts)) != state.numParts {
		t.Errorf("expected %d parts, got %d", state.numParts, len(parts))
	}

	if uploadCount.Load() != int32(state.numParts) {
		t.Errorf("expected %d uploads, got %d", state.numParts, uploadCount.Load())
	}

	if overallHash == "" {
		t.Error("overall hash should not be empty")
	}

	for i, part := range parts {
		expectedPartNumber := i + 1
		if part.PartNumber != expectedPartNumber {
			t.Errorf("part %d: expected PartNumber %d, got %d", i, expectedPartNumber, part.PartNumber)
		}

		if part.ETag == "" {
			t.Errorf("part %d: ETag should not be empty", i)
		}
	}

	etagMap := make(map[string]bool)
	for _, etag := range receivedETags {
		if etagMap[etag] {
			t.Errorf("duplicate ETag found: %s", etag)
		}
		etagMap[etag] = true
	}
}

// TestRetryableErrorDetection tests the retry logic for different error types
func TestRetryableErrorDetection(t *testing.T) {
	testCases := []struct {
		name       string
		err        error
		shouldRetry bool
	}{
		{
			name:        "nil error should not retry",
			err:         nil,
			shouldRetry: false,
		},
		{
			name:        "400 error should not retry",
			err:         fmt.Errorf("bad request: 400"),
			shouldRetry: false,
		},
		{
			name:        "401 error should not retry",
			err:         fmt.Errorf("unauthorized: 401"),
			shouldRetry: false,
		},
		{
			name:        "403 error should not retry",
			err:         fmt.Errorf("forbidden: 403"),
			shouldRetry: false,
		},
		{
			name:        "404 error should not retry",
			err:         fmt.Errorf("not found: 404"),
			shouldRetry: false,
		},
		{
			name:        "500 error should retry",
			err:         fmt.Errorf("internal server error: 500"),
			shouldRetry: true,
		},
		{
			name:        "502 error should retry",
			err:         fmt.Errorf("bad gateway: 502"),
			shouldRetry: true,
		},
		{
			name:        "503 error should retry",
			err:         fmt.Errorf("service unavailable: 503"),
			shouldRetry: true,
		},
		{
			name:        "network timeout should retry",
			err:         fmt.Errorf("connection timeout"),
			shouldRetry: true,
		},
		{
			name:        "generic error should retry",
			err:         fmt.Errorf("some random error"),
			shouldRetry: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isRetryableError(tc.err)
			if result != tc.shouldRetry {
				t.Errorf("expected retryable=%v for error %v, got %v", tc.shouldRetry, tc.err, result)
			}
		})
	}
}

// TestChunkRetryLogic tests that failed uploads are retried
func TestChunkRetryLogic(t *testing.T) {
	cfg := &config.Config{
		Mnemonic:   TestMnemonic,
		Bucket:     TestBucket6,
		HTTPClient: &http.Client{},
	}

	state, err := newMultipartUploadState(cfg, 100*1024*1024)
	if err != nil {
		t.Fatalf("newMultipartUploadState failed: %v", err)
	}

	var attemptCount atomic.Int32

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts := attemptCount.Add(1)

		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("temporary failure"))
			return
		}

		w.Header().Set("ETag", "\"success-etag\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	state.startResp = &StartUploadResp{
		Uploads: []UploadPart{
			{URLs: []string{mockServer.URL}},
		},
	}

	testData := []byte("test data")
	etag, err := state.uploadChunkWithRetry(context.Background(), 0, testData)

	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}

	if etag != "success-etag" {
		t.Errorf("expected ETag 'success-etag', got '%s'", etag)
	}

	if attemptCount.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attemptCount.Load())
	}
}

// TestChunkRetryExhaustion tests that non-retryable errors fail immediately
func TestChunkRetryExhaustion(t *testing.T) {
	cfg := &config.Config{
		Mnemonic:   TestMnemonic,
		Bucket:     TestBucket6,
		HTTPClient: &http.Client{},
	}

	state, err := newMultipartUploadState(cfg, 100*1024*1024)
	if err != nil {
		t.Fatalf("newMultipartUploadState failed: %v", err)
	}

	var attemptCount atomic.Int32

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount.Add(1)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found: 404"))
	}))
	defer mockServer.Close()

	state.startResp = &StartUploadResp{
		Uploads: []UploadPart{
			{URLs: []string{mockServer.URL}},
		},
	}

	testData := []byte("test data")
	_, err = state.uploadChunkWithRetry(context.Background(), 0, testData)

	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}

	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected error to contain '404', got: %v", err)
	}

	if attemptCount.Load() != 1 {
		t.Errorf("expected 1 attempt for non-retryable error, got %d", attemptCount.Load())
	}
}

func TestContainsHelper(t *testing.T) {
	testCases := []struct {
		str      string
		substr   string
		expected bool
	}{
		{"hello world", "world", true},
		{"hello world", "hello", true},
		{"hello world", "lo wo", true},
		{"hello world", "xyz", false},
		{"", "test", false},
		{"test", "", true},
		{"", "", true},
		{"status: 404", "404", true},
		{"error 500 occurred", "500", true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("'%s' contains '%s'", tc.str, tc.substr), func(t *testing.T) {
			result := contains(tc.str, tc.substr)
			if result != tc.expected {
				t.Errorf("contains('%s', '%s') = %v, expected %v", tc.str, tc.substr, result, tc.expected)
			}
		})
	}
}
