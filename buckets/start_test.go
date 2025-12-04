package buckets

import (
	"encoding/json"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
)

// TestStartUploadMultipart tests the multipart upload start functionality
func TestStartUploadMultipart(t *testing.T) {
	testCases := []struct {
		name           string
		numParts       int
		fileSize       int64
		mockResponse   StartUploadResp
		mockStatusCode int
		expectError    bool
	}{
		{
			name:     "successful multipart start - 4 parts",
			numParts: 4,
			fileSize: 120 * 1024 * 1024, // 120 MB
			mockResponse: StartUploadResp{
				Uploads: []UploadPart{
					{
						Index:    0,
						UUID:     "test-uuid-123",
						URLs:     []string{"https://s3.example.com/url1", "https://s3.example.com/url2", "https://s3.example.com/url3", "https://s3.example.com/url4"},
						UploadId: "test-upload-id-456",
					},
				},
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:     "successful multipart start - 10 parts",
			numParts: 10,
			fileSize: 300 * 1024 * 1024, // 300 MB
			mockResponse: StartUploadResp{
				Uploads: []UploadPart{
					{
						Index:    0,
						UUID:     "test-uuid-789",
						URLs:     make([]string, 10),
						UploadId: "test-upload-id-789",
					},
				},
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "server error - 500",
			numParts:       4,
			fileSize:       120 * 1024 * 1024,
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
		},
		{
			name:           "unauthorized - 401",
			numParts:       4,
			fileSize:       120 * 1024 * 1024,
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
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
				if r.Header.Get("internxt-client") != "rclone" {
					t.Errorf("expected internxt-client rclone, got %s", r.Header.Get("internxt-client"))
				}
				if r.Header.Get("Content-Type") != "application/json; charset=utf-8" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}

				multiparts := r.URL.Query().Get("multiparts")
				if !tc.expectError && multiparts == "" {
					t.Error("multiparts query parameter missing")
				}

				var reqBody startUploadReq
				if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil && !tc.expectError {
					t.Errorf("failed to decode request body: %v", err)
				}

				if !tc.expectError {
					if len(reqBody.Uploads) != 1 {
						t.Errorf("expected 1 upload spec, got %d", len(reqBody.Uploads))
					}
					if reqBody.Uploads[0].Index != 0 {
						t.Errorf("expected index 0, got %d", reqBody.Uploads[0].Index)
					}
					if reqBody.Uploads[0].Size != tc.fileSize {
						t.Errorf("expected size %d, got %d", tc.fileSize, reqBody.Uploads[0].Size)
					}
				}

				w.WriteHeader(tc.mockStatusCode)
				if tc.mockStatusCode == http.StatusOK {
					json.NewEncoder(w).Encode(tc.mockResponse)
				} else {
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			cfg := &config.Config{
				BasicAuthHeader: "Basic test-auth",
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL),
			}

			specs := []UploadPartSpec{
				{Index: 0, Size: tc.fileSize},
			}

			result, err := StartUploadMultipart(context.Background(), cfg, "test-bucket", specs, tc.numParts)

			if tc.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "failed") && !strings.Contains(err.Error(), "status") {
					t.Errorf("unexpected error message: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if len(result.Uploads) != 1 {
					t.Errorf("expected 1 upload entry, got %d", len(result.Uploads))
				}

				upload := result.Uploads[0]
				if upload.UUID != tc.mockResponse.Uploads[0].UUID {
					t.Errorf("expected UUID %s, got %s", tc.mockResponse.Uploads[0].UUID, upload.UUID)
				}
				if upload.UploadId != tc.mockResponse.Uploads[0].UploadId {
					t.Errorf("expected UploadId %s, got %s", tc.mockResponse.Uploads[0].UploadId, upload.UploadId)
				}
				if len(upload.URLs) != tc.numParts {
					t.Errorf("expected %d URLs, got %d", tc.numParts, len(upload.URLs))
				}
			}
		})
	}
}

// TestStartUploadMultipartRequestFormat tests the request format details
func TestStartUploadMultipartRequestFormat(t *testing.T) {
	requestReceived := false
	var capturedBody startUploadReq
	var capturedMultiparts string

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		capturedMultiparts = r.URL.Query().Get("multiparts")

		json.NewDecoder(r.Body).Decode(&capturedBody)

		response := StartUploadResp{
			Uploads: []UploadPart{
				{
					UUID:     "uuid",
					UploadId: "upload-id",
					URLs:     []string{"url1", "url2"},
				},
			},
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

	specs := []UploadPartSpec{
		{Index: 0, Size: 100 * 1024 * 1024},
	}

	_, err := StartUploadMultipart(context.Background(), cfg, "bucket-123", specs, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !requestReceived {
		t.Fatal("request was not received by mock server")
	}

	if capturedMultiparts != "2" {
		t.Errorf("expected multiparts=2, got multiparts=%s", capturedMultiparts)
	}

	if len(capturedBody.Uploads) != 1 {
		t.Errorf("expected 1 upload spec in body, got %d", len(capturedBody.Uploads))
	}

	if capturedBody.Uploads[0].Index != 0 {
		t.Errorf("expected spec index 0, got %d", capturedBody.Uploads[0].Index)
	}

	if capturedBody.Uploads[0].Size != 100*1024*1024 {
		t.Errorf("expected spec size 100MB, got %d", capturedBody.Uploads[0].Size)
	}
}
