package files

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
	"github.com/internxt/rclone-adapter/buckets"
)

func TestDeleteFile(t *testing.T) {
	testCases := []struct {
		name           string
		uuid           string
		mockStatusCode int
		expectError    bool
		errorContains  string
	}{
		{
			name:           "successful deletion",
			uuid:           buckets.TestFileUUID,
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "unauthorized - 401",
			uuid:           buckets.TestFileUUID,
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
			errorContains:  "401",
		},
		{
			name:           "not found - 404",
			uuid:           "non-existent-uuid",
			mockStatusCode: http.StatusNotFound,
			expectError:    true,
			errorContains:  "404",
		},
		{
			name:           "server error - 500",
			uuid:           buckets.TestFileUUID,
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "500",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "DELETE" {
					t.Errorf("expected DELETE request, got %s", r.Method)
				}

				authHeader := r.Header.Get("Authorization")
				if !strings.HasPrefix(authHeader, "Bearer ") {
					t.Error("expected Authorization header with Bearer token")
				}

				if !strings.Contains(r.URL.Path, tc.uuid) {
					t.Errorf("expected path to contain %s, got %s", tc.uuid, r.URL.Path)
				}

				w.WriteHeader(tc.mockStatusCode)
				if tc.mockStatusCode != http.StatusOK {
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			cfg := &config.Config{
				Token:     "test-token",
				Endpoints: endpoints.NewConfig(mockServer.URL),
			}
			cfg.ApplyDefaults()

			err := DeleteFile(context.Background(), cfg, tc.uuid)

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
			}
		})
	}
}

func TestRenameFile(t *testing.T) {
	testCases := []struct {
		name           string
		fileUUID       string
		newPlainName   string
		newType        string
		mockStatusCode int
		expectError    bool
		errorContains  string
	}{
		{
			name:           "successful rename with type",
			fileUUID:       buckets.TestFileUUID,
			newPlainName:   "new-name",
			newType:        "text/plain",
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "successful rename without type",
			fileUUID:       buckets.TestFileUUID,
			newPlainName:   "new-name",
			newType:        "",
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "unauthorized - 401",
			fileUUID:       buckets.TestFileUUID,
			newPlainName:   "new-name",
			newType:        "",
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
			errorContains:  "401",
		},
		{
			name:           "not found - 404",
			fileUUID:       "non-existent-uuid",
			newPlainName:   "new-name",
			newType:        "",
			mockStatusCode: http.StatusNotFound,
			expectError:    true,
			errorContains:  "404",
		},
		{
			name:           "server error - 500",
			fileUUID:       buckets.TestFileUUID,
			newPlainName:   "new-name",
			newType:        "",
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "500",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedPayload map[string]string

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PUT" {
					t.Errorf("expected PUT request, got %s", r.Method)
				}

				authHeader := r.Header.Get("Authorization")
				if !strings.HasPrefix(authHeader, "Bearer ") {
					t.Error("expected Authorization header with Bearer token")
				}

				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}

				if !strings.Contains(r.URL.Path, tc.fileUUID) || !strings.Contains(r.URL.Path, "/meta") {
					t.Errorf("expected path to contain %s and /meta, got %s", tc.fileUUID, r.URL.Path)
				}

				if err := json.NewDecoder(r.Body).Decode(&capturedPayload); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}

				w.WriteHeader(tc.mockStatusCode)
				if tc.mockStatusCode != http.StatusOK {
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			cfg := &config.Config{
				Token:     "test-token",
				Endpoints: endpoints.NewConfig(mockServer.URL),
			}
			cfg.ApplyDefaults()

			err := RenameFile(context.Background(), cfg, tc.fileUUID, tc.newPlainName, tc.newType)

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

				if capturedPayload["plainName"] != tc.newPlainName {
					t.Errorf("expected plainName %s, got %s", tc.newPlainName, capturedPayload["plainName"])
				}

				if tc.newType != "" {
					if capturedPayload["type"] != tc.newType {
						t.Errorf("expected type %s, got %s", tc.newType, capturedPayload["type"])
					}
				} else {
					if _, ok := capturedPayload["type"]; ok {
						t.Error("expected type field to be omitted when empty, but it was present")
					}
				}
			}
		})
	}
}
