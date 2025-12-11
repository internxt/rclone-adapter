package buckets

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
)

// TestFinishUpload tests the single-part upload completion functionality
func TestFinishUpload(t *testing.T) {
	testCases := []struct {
		name           string
		bucketID       string
		index          string
		shards         []Shard
		mockResponse   FinishUploadResp
		mockStatusCode int
		mockBody       string
		expectError    bool
		errorContains  string
	}{
		{
			name:     "successful single shard upload",
			bucketID: TestBucket1,
			index:    "abc123def456",
			shards: []Shard{
				{Hash: "hash1", UUID: "uuid1"},
			},
			mockResponse: FinishUploadResp{
				Bucket:   TestBucket1,
				Index:    "abc123def456",
				ID:       TestFileID,
				Version:  1,
				Created:  "2025-01-01T00:00:00Z",
				Mimetype: "application/octet-stream",
				Filename: "test-file",
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:     "successful multiple shards",
			bucketID: TestBucket2,
			index:    "multi-shard-index",
			shards: []Shard{
				{Hash: "hash1", UUID: "uuid1"},
				{Hash: "hash2", UUID: "uuid2"},
				{Hash: "hash3", UUID: "uuid3"},
			},
			mockResponse: FinishUploadResp{
				ID:      "multi-file-id",
				Bucket:  TestBucket2,
				Version: 2,
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "error - duplicate key",
			bucketID:       TestBucket3,
			index:          "duplicate-index",
			shards:         []Shard{{Hash: "hash", UUID: "uuid"}},
			mockStatusCode: http.StatusInternalServerError,
			mockBody:       `{"error": "duplicate key error collection: buckets"}`,
			expectError:    true,
			errorContains:  "duplicate shard",
		},
		{
			name:           "error - server 500",
			bucketID:       TestBucket4,
			index:          "error-index",
			shards:         []Shard{{Hash: "hash", UUID: "uuid"}},
			mockStatusCode: http.StatusInternalServerError,
			mockBody:       "internal server error",
			expectError:    true,
			errorContains:  "finish upload failed",
		},
		{
			name:           "error - unauthorized 401",
			bucketID:       TestBucket5,
			index:          "unauth-index",
			shards:         []Shard{{Hash: "hash", UUID: "uuid"}},
			mockStatusCode: http.StatusUnauthorized,
			mockBody:       "unauthorized",
			expectError:    true,
			errorContains:  "401",
		},
		{
			name:           "error - bad request 400",
			bucketID:       TestBucket6,
			index:          "bad-index",
			shards:         []Shard{},
			mockStatusCode: http.StatusBadRequest,
			mockBody:       "invalid request",
			expectError:    true,
			errorContains:  "400",
		},
		{
			name:           "error - not found 404",
			bucketID:       "nonexistent-bucket",
			index:          "index",
			shards:         []Shard{{Hash: "hash", UUID: "uuid"}},
			mockStatusCode: http.StatusNotFound,
			mockBody:       "bucket not found",
			expectError:    true,
			errorContains:  "404",
		},
		{
			name:           "error - invalid JSON response",
			bucketID:       TestBucket1,
			index:          "valid-index",
			shards:         []Shard{{Hash: "hash", UUID: "uuid"}},
			mockStatusCode: http.StatusOK,
			mockBody:       "invalid json {{{",
			expectError:    true,
			errorContains:  "failed to unmarshal",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("expected POST request, got %s", r.Method)
				}

				if r.Header.Get("Authorization") == "" {
					t.Error("Authorization header missing")
				}
				if r.Header.Get("internxt-version") != "1.0" {
					t.Errorf("expected internxt-version 1.0, got %s", r.Header.Get("internxt-version"))
				}
				if r.Header.Get("internxt-client") != "drive-web" {
					t.Errorf("expected internxt-client drive-web, got %s", r.Header.Get("internxt-client"))
				}
				if r.Header.Get("Content-Type") != "application/json; charset=utf-8" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}

				var reqBody map[string]any
				if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil && !tc.expectError {
					t.Errorf("failed to decode request body: %v", err)
				}

				if !tc.expectError || tc.mockStatusCode == http.StatusOK {
					if reqBody["index"] != tc.index {
						t.Errorf("expected index %s, got %v", tc.index, reqBody["index"])
					}

					shards, ok := reqBody["shards"].([]any)
					if !ok {
						t.Error("shards field missing or not an array")
					} else if len(shards) != len(tc.shards) {
						t.Errorf("expected %d shards, got %d", len(tc.shards), len(shards))
					}
				}

				w.WriteHeader(tc.mockStatusCode)
				if tc.mockBody != "" {
					w.Write([]byte(tc.mockBody))
				} else if tc.mockStatusCode == http.StatusOK {
					json.NewEncoder(w).Encode(tc.mockResponse)
				} else {
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			cfg := &config.Config{
				BasicAuthHeader: TestBasicAuth,
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL),
			}

			result, err := FinishUpload(context.Background(), cfg, tc.bucketID, tc.index, tc.shards)

			if tc.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain '%s', got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result == nil {
					t.Fatal("expected result, got nil")
				}
				if result.ID != tc.mockResponse.ID {
					t.Errorf("expected ID %s, got %s", tc.mockResponse.ID, result.ID)
				}
				if tc.mockResponse.Bucket != "" && result.Bucket != tc.mockResponse.Bucket {
					t.Errorf("expected Bucket %s, got %s", tc.mockResponse.Bucket, result.Bucket)
				}
			}
		})
	}
}

// TestFinishUploadContextCancellation tests context cancellation handling
func TestFinishUploadContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &config.Config{
		BasicAuthHeader: TestBasicAuth,
		HTTPClient:      &http.Client{},
		Endpoints:       endpoints.NewConfig("http://localhost:9999"),
	}

	_, err := FinishUpload(ctx, cfg, TestBucket1, "index", []Shard{{Hash: "hash", UUID: "uuid"}})
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

// TestFinishUploadRequestPayload tests the exact payload structure
func TestFinishUploadRequestPayload(t *testing.T) {
	var capturedPayload map[string]any
	var capturedBody []byte

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		json.Unmarshal(capturedBody, &capturedPayload)

		response := FinishUploadResp{ID: "test-id"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Config{
		BasicAuthHeader: TestBasicAuth,
		HTTPClient:      &http.Client{},
		Endpoints:       endpoints.NewConfig(mockServer.URL),
	}

	shards := []Shard{
		{Hash: "hash1", UUID: "uuid1"},
		{Hash: "hash2", UUID: "uuid2"},
	}

	_, err := FinishUpload(context.Background(), cfg, TestBucket1, "test-index-789", shards)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedPayload["index"] != "test-index-789" {
		t.Errorf("expected index 'test-index-789', got %v", capturedPayload["index"])
	}

	shardsData, ok := capturedPayload["shards"].([]any)
	if !ok {
		t.Fatal("shards field missing or not an array")
	}

	if len(shardsData) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(shardsData))
	}

	// Verify first shard
	shard1 := shardsData[0].(map[string]any)
	if shard1["hash"] != "hash1" {
		t.Errorf("expected hash 'hash1', got %v", shard1["hash"])
	}
	if shard1["uuid"] != "uuid1" {
		t.Errorf("expected uuid 'uuid1', got %v", shard1["uuid"])
	}
}

// TestFinishMultipartUpload tests the multipart upload completion functionality
func TestFinishMultipartUpload(t *testing.T) {
	testCases := []struct {
		name           string
		shard          MultipartShard
		mockResponse   FinishUploadResp
		mockStatusCode int
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful completion with 4 parts",
			shard: MultipartShard{
				UUID:     "test-uuid-123",
				Hash:     "abc123def456",
				UploadId: "upload-id-789",
				Parts: []CompletedPart{
					{PartNumber: 1, ETag: "etag1"},
					{PartNumber: 2, ETag: "etag2"},
					{PartNumber: 3, ETag: "etag3"},
					{PartNumber: 4, ETag: "etag4"},
				},
			},
			mockResponse: FinishUploadResp{
				Bucket:   "test-bucket",
				Index:    "0123456789abcdef",
				ID:       "file-id-123",
				Version:  1,
				Created:  "2025-01-01T00:00:00Z",
				Mimetype: "application/octet-stream",
				Filename: "test-file",
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name: "successful completion with many parts",
			shard: MultipartShard{
				UUID:     "uuid-large-file",
				Hash:     "hash-large-file",
				UploadId: "upload-large",
				Parts:    make([]CompletedPart, 20),
			},
			mockResponse: FinishUploadResp{
				ID: "file-id-large",
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name: "duplicate key error",
			shard: MultipartShard{
				UUID:     "duplicate-uuid",
				Hash:     "duplicate-hash",
				UploadId: "duplicate-upload",
				Parts:    []CompletedPart{{PartNumber: 1, ETag: "etag"}},
			},
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "duplicate",
		},
		{
			name: "server error - 500",
			shard: MultipartShard{
				UUID:     "error-uuid",
				Hash:     "error-hash",
				UploadId: "error-upload",
				Parts:    []CompletedPart{{PartNumber: 1, ETag: "etag"}},
			},
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "failed",
		},
		{
			name: "unauthorized - 401",
			shard: MultipartShard{
				UUID:     "unauth-uuid",
				Hash:     "unauth-hash",
				UploadId: "unauth-upload",
				Parts:    []CompletedPart{{PartNumber: 1, ETag: "etag"}},
			},
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
			errorContains:  "401",
		},
		{
			name: "bad request - 400",
			shard: MultipartShard{
				UUID:     "bad-uuid",
				Hash:     "bad-hash",
				UploadId: "bad-upload",
				Parts:    []CompletedPart{},
			},
			mockStatusCode: http.StatusBadRequest,
			expectError:    true,
			errorContains:  "400",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize Parts array for test cases that need it
			if len(tc.shard.Parts) == 20 {
				for i := 0; i < 20; i++ {
					tc.shard.Parts[i] = CompletedPart{
						PartNumber: i + 1,
						ETag:       "etag-" + string(rune(i)),
					}
				}
			}

			// Create mock server
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Verify request method
				if r.Method != "POST" {
					t.Errorf("expected POST request, got %s", r.Method)
				}

				// Verify headers
				if r.Header.Get("Authorization") == "" {
					t.Error("Authorization header missing")
				}
				if r.Header.Get("internxt-version") != "1.0" {
					t.Errorf("expected internxt-version 1.0, got %s", r.Header.Get("internxt-version"))
				}
				if r.Header.Get("internxt-client") != "rclone" {
					t.Errorf("expected internxt-client rclone, got %s", r.Header.Get("internxt-client"))
				}
				if r.Header.Get("Content-Type") != "application/json; charset=utf-8" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}

				// Verify request body structure
				var reqBody map[string]any
				if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil && !tc.expectError {
					t.Errorf("failed to decode request body: %v", err)
				}

				if !tc.expectError {
					// Verify index field
					if _, ok := reqBody["index"]; !ok {
						t.Error("index field missing from request body")
					}

					// Verify shards field is an array
					shards, ok := reqBody["shards"].([]any)
					if !ok {
						t.Error("shards field missing or not an array")
					} else if len(shards) != 1 {
						t.Errorf("expected 1 shard, got %d", len(shards))
					} else {
						// Verify shard structure
						shard := shards[0].(map[string]any)
						if shard["uuid"] != tc.shard.UUID {
							t.Errorf("expected UUID %s, got %v", tc.shard.UUID, shard["uuid"])
						}
						if shard["hash"] != tc.shard.Hash {
							t.Errorf("expected Hash %s, got %v", tc.shard.Hash, shard["hash"])
						}
						if shard["UploadId"] != tc.shard.UploadId {
							t.Errorf("expected UploadId %s, got %v", tc.shard.UploadId, shard["UploadId"])
						}

						// Verify parts array
						parts, ok := shard["parts"].([]any)
						if !ok {
							t.Error("parts field missing or not an array")
						} else if len(parts) != len(tc.shard.Parts) {
							t.Errorf("expected %d parts, got %d", len(tc.shard.Parts), len(parts))
						}
					}
				}

				// Send response
				w.WriteHeader(tc.mockStatusCode)
				if tc.mockStatusCode == http.StatusOK {
					json.NewEncoder(w).Encode(tc.mockResponse)
				} else {
					if tc.errorContains == "duplicate" {
						w.Write([]byte(`{"error": "duplicate key error"}`))
					} else {
						w.Write([]byte("error message"))
					}
				}
			}))
			defer mockServer.Close()

			// Create config with mock endpoint
			cfg := &config.Config{
				BasicAuthHeader: TestBasicAuth,
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL),
			}

			// Call FinishMultipartUpload
			result, err := FinishMultipartUpload(context.Background(), cfg, "test-bucket", "test-index", tc.shard)

			// Verify results
			if tc.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain '%s', got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if result.ID != tc.mockResponse.ID {
					t.Errorf("expected ID %s, got %s", tc.mockResponse.ID, result.ID)
				}
				if tc.mockResponse.Bucket != "" && result.Bucket != tc.mockResponse.Bucket {
					t.Errorf("expected Bucket %s, got %s", tc.mockResponse.Bucket, result.Bucket)
				}
			}
		})
	}
}

// TestFinishMultipartUploadPayloadStructure tests the exact payload structure
func TestFinishMultipartUploadPayloadStructure(t *testing.T) {
	var capturedPayload map[string]any

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&capturedPayload)

		response := FinishUploadResp{
			ID: "test-id",
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := &config.Config{
		BasicAuthHeader: "Basic test",
		HTTPClient:      &http.Client{},
		Endpoints:       endpoints.NewConfig(mockServer.URL),
	}

	shard := MultipartShard{
		UUID:     "uuid-123",
		Hash:     "hash-abc",
		UploadId: "upload-xyz",
		Parts: []CompletedPart{
			{PartNumber: 1, ETag: "etag1"},
			{PartNumber: 2, ETag: "etag2"},
		},
	}

	_, err := FinishMultipartUpload(context.Background(), cfg, "bucket-123", "index-456", shard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedPayload["index"] != "index-456" {
		t.Errorf("expected index 'index-456', got %v", capturedPayload["index"])
	}

	shards, ok := capturedPayload["shards"].([]any)
	if !ok {
		t.Fatal("shards field missing or not an array")
	}

	if len(shards) != 1 {
		t.Fatalf("expected 1 shard in payload, got %d", len(shards))
	}

	shardData := shards[0].(map[string]any)

	expectedFields := []string{"uuid", "hash", "UploadId", "parts"}
	for _, field := range expectedFields {
		if _, ok := shardData[field]; !ok {
			t.Errorf("expected field '%s' in shard, not found", field)
		}
	}

	parts, ok := shardData["parts"].([]any)
	if !ok {
		t.Fatal("parts field missing or not an array")
	}

	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}

	part1 := parts[0].(map[string]any)
	if part1["PartNumber"] != float64(1) { // JSON unmarshals numbers as float64
		t.Errorf("expected PartNumber 1, got %v", part1["PartNumber"])
	}
	if part1["ETag"] != "etag1" {
		t.Errorf("expected ETag 'etag1', got %v", part1["ETag"])
	}
}

// TestFinishMultipartUploadEmptyParts tests handling of empty parts array
func TestFinishMultipartUploadEmptyParts(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid parts"))
	}))
	defer mockServer.Close()

	cfg := &config.Config{
		BasicAuthHeader: "Basic test",
		HTTPClient:      &http.Client{},
		Endpoints:       endpoints.NewConfig(mockServer.URL),
	}

	shard := MultipartShard{
		UUID:     "uuid",
		Hash:     "hash",
		UploadId: "upload",
		Parts:    []CompletedPart{}, // Empty parts
	}

	_, err := FinishMultipartUpload(context.Background(), cfg, "bucket", "index", shard)

	if err == nil {
		t.Error("expected error for empty parts, got nil")
	}
}
