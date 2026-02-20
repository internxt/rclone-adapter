package buckets

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/config"
)

func TestCreateMetaFile(t *testing.T) {
	fileID := TestFileID
	testCases := []struct {
		name           string
		request        CreateMetaRequest
		mockResponse   CreateMetaResponse
		mockStatusCode int
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful creation",
			request: CreateMetaRequest{
				Name:             TestFileNameNoExt,
				Bucket:           TestBucket1,
				FileID:           &fileID,
				EncryptVersion:   "03-aes",
				FolderUuid:       TestFolderUUID,
				Size:             1024,
				PlainName:        TestFileNameNoExt,
				Type:             "txt",
				CreationTime:     time.Now(),
				Date:             time.Now(),
				ModificationTime: time.Now(),
			},
			mockResponse: CreateMetaResponse{
				UUID:           TestFileUUID2,
				Name:           TestFileNameNoExt,
				Bucket:         TestBucket1,
				FileID:         TestFileID,
				EncryptVersion: "03-aes",
				FolderUuid:     TestFolderUUID,
				Size:           json.Number("1024"),
				PlainName:      TestFileNameNoExt,
				Type:           "txt",
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name: "error - 401 unauthorized",
			request: CreateMetaRequest{
				Name:             TestFileNameNoExt,
				Bucket:           TestBucket1,
				FileID:           &fileID,
				EncryptVersion:   "03-aes",
				FolderUuid:       TestFolderUUID,
				Size:             1024,
				PlainName:        TestFileNameNoExt,
				Type:             "txt",
				CreationTime:     time.Now(),
				Date:             time.Now(),
				ModificationTime: time.Now(),
			},
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
			errorContains:  "401",
		},
		{
			name: "error - 500 server error",
			request: CreateMetaRequest{
				Name:             TestFileNameNoExt,
				Bucket:           TestBucket1,
				FileID:           &fileID,
				EncryptVersion:   "03-aes",
				FolderUuid:       TestFolderUUID,
				Size:             1024,
				PlainName:        TestFileNameNoExt,
				Type:             "txt",
				CreationTime:     time.Now(),
				Date:             time.Now(),
				ModificationTime: time.Now(),
			},
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "500",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedRequest CreateMetaRequest

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("expected POST request, got %s", r.Method)
				}

				authHeader := r.Header.Get("Authorization")
				if !strings.HasPrefix(authHeader, "Bearer ") {
					t.Error("expected Authorization header with Bearer token")
				}

				if r.Header.Get("internxt-version") != "v1.0.436" {
					t.Errorf("expected internxt-version v1.0.436, got %s", r.Header.Get("internxt-version"))
				}

				if r.Header.Get("internxt-client") != config.ClientName {
					t.Errorf("expected internxt-client %s, got %s", config.ClientName, r.Header.Get("internxt-client"))
				}

				if r.Header.Get("Content-Type") != "application/json; charset=utf-8" {
					t.Errorf("expected Content-Type application/json; charset=utf-8, got %s", r.Header.Get("Content-Type"))
				}

				if err := json.NewDecoder(r.Body).Decode(&capturedRequest); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}

				w.WriteHeader(tc.mockStatusCode)
				if tc.mockStatusCode == http.StatusOK {
					json.NewEncoder(w).Encode(tc.mockResponse)
				} else {
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			cfg := newTestConfig(mockServer.URL)

			result, err := CreateMetaFile(
				context.Background(),
				cfg,
				tc.request.Name,
				tc.request.Bucket,
				tc.request.FileID,
				tc.request.EncryptVersion,
				tc.request.FolderUuid,
				tc.request.PlainName,
				tc.request.Type,
				tc.request.Size,
				tc.request.ModificationTime,
			)

			if tc.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, got %q", tc.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if result.UUID != tc.mockResponse.UUID {
					t.Errorf("expected UUID %s, got %s", tc.mockResponse.UUID, result.UUID)
				}
				if result.FileID != tc.mockResponse.FileID {
					t.Errorf("expected FileID %s, got %s", tc.mockResponse.FileID, result.FileID)
				}
			}
		})
	}
}

func TestCreateMetaFileInvalidJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json"))
	}))
	defer mockServer.Close()

	cfg := newTestConfig(mockServer.URL)

	fileID := TestFileID2
	_, err := CreateMetaFile(
		context.Background(),
		cfg,
		TestFileNameNoExt,
		TestBucket1,
		&fileID,
		"03-aes",
		TestFolderUUID,
		TestFileNameNoExt,
		"txt",
		1024,
		time.Now(),
	)

	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to unmarshal") {
		t.Errorf("expected error to contain 'failed to unmarshal', got %q", err.Error())
	}
}

func TestCreateMetaFileRetryOn404(t *testing.T) {
	fileID := TestFileID

	t.Run("retries once on 404 then succeeds", func(t *testing.T) {
		var callCount atomic.Int32
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := callCount.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"message":"Folder not found"}`))
				return
			}
			resp := CreateMetaResponse{UUID: TestFileUUID2, FileID: TestFileID}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)
		result, err := CreateMetaFile(context.Background(), cfg,
			TestFileNameNoExt, TestBucket1, &fileID, "03-aes",
			TestFolderUUID, TestFileNameNoExt, "txt", 1024, time.Now())

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.UUID != TestFileUUID2 {
			t.Errorf("expected UUID %s, got %s", TestFileUUID2, result.UUID)
		}
		if callCount.Load() != 2 {
			t.Errorf("expected 2 calls (1 failure + 1 retry), got %d", callCount.Load())
		}
	})

	t.Run("fails after retry on persistent 404", func(t *testing.T) {
		var callCount atomic.Int32
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount.Add(1)
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"message":"Folder not found"}`))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)
		_, err := CreateMetaFile(context.Background(), cfg,
			TestFileNameNoExt, TestBucket1, &fileID, "03-aes",
			TestFolderUUID, TestFileNameNoExt, "txt", 1024, time.Now())

		if err == nil {
			t.Fatal("expected error after retry, got nil")
		}
		if callCount.Load() != 2 {
			t.Errorf("expected 2 calls (1 failure + 1 retry), got %d", callCount.Load())
		}
	})

	t.Run("does not retry on non-404 errors", func(t *testing.T) {
		var callCount atomic.Int32
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount.Add(1)
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"message":"Access denied"}`))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)
		_, err := CreateMetaFile(context.Background(), cfg,
			TestFileNameNoExt, TestBucket1, &fileID, "03-aes",
			TestFolderUUID, TestFileNameNoExt, "txt", 1024, time.Now())

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if callCount.Load() != 1 {
			t.Errorf("expected exactly 1 call (no retry), got %d", callCount.Load())
		}
	})

	t.Run("respects context cancellation during retry wait", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"message":"Folder not found"}`))
		}))
		defer mockServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		cfg := newTestConfig(mockServer.URL)
		_, err := CreateMetaFile(ctx, cfg,
			TestFileNameNoExt, TestBucket1, &fileID, "03-aes",
			TestFolderUUID, TestFileNameNoExt, "txt", 1024, time.Now())

		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
