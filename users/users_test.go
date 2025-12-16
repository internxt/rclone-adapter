package users

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
)

func TestGetUsage(t *testing.T) {
	testCases := []struct {
		name           string
		mockResponse   UsageResponse
		mockStatusCode int
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful usage retrieval",
			mockResponse: UsageResponse{
				Drive: 1024 * 1024 * 1024, // 1 GB
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name: "zero usage",
			mockResponse: UsageResponse{
				Drive: 0,
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "unauthorized - 401",
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
			errorContains:  "401",
		},
		{
			name:           "server error - 500",
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "500",
		},
		{
			name:           "forbidden - 403",
			mockStatusCode: http.StatusForbidden,
			expectError:    true,
			errorContains:  "403",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("expected GET request, got %s", r.Method)
				}

				authHeader := r.Header.Get("Authorization")
				if !strings.HasPrefix(authHeader, "Bearer ") {
					t.Error("expected Authorization header with Bearer token")
				}

				if !strings.Contains(r.URL.Path, "/usage") {
					t.Errorf("expected path to contain /usage, got %s", r.URL.Path)
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
				Token:     "test-token",
				Endpoints: endpoints.NewConfig(mockServer.URL),
			}
			cfg.ApplyDefaults()

			usage, err := GetUsage(context.Background(), cfg)

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
				if usage == nil {
					t.Fatal("expected usage response, got nil")
				}
				if usage.Drive != tc.mockResponse.Drive {
					t.Errorf("expected Drive %d, got %d", tc.mockResponse.Drive, usage.Drive)
				}
			}
		})
	}
}

func TestGetUsageInvalidJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json response"))
	}))
	defer mockServer.Close()

	cfg := &config.Config{
		Token:     "test-token",
		Endpoints: endpoints.NewConfig(mockServer.URL),
	}
	cfg.ApplyDefaults()

	_, err := GetUsage(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode") {
		t.Errorf("expected error to contain 'failed to decode', got %q", err.Error())
	}
}

func TestGetLimit(t *testing.T) {
	testCases := []struct {
		name           string
		mockResponse   LimitResponse
		mockStatusCode int
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful limit retrieval",
			mockResponse: LimitResponse{
				MaxSpaceBytes: 10 * 1024 * 1024 * 1024, // 10 GB
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name: "zero limit",
			mockResponse: LimitResponse{
				MaxSpaceBytes: 0,
			},
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "unauthorized - 401",
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
			errorContains:  "401",
		},
		{
			name:           "server error - 500",
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "500",
		},
		{
			name:           "not found - 404",
			mockStatusCode: http.StatusNotFound,
			expectError:    true,
			errorContains:  "404",	
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("expected GET request, got %s", r.Method)
				}

				authHeader := r.Header.Get("Authorization")
				if !strings.HasPrefix(authHeader, "Bearer ") {
					t.Error("expected Authorization header with Bearer token")
				}

				if !strings.Contains(r.URL.Path, "/limit") {
					t.Errorf("expected path to contain /limit, got %s", r.URL.Path)
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
				Token:     "test-token",
				Endpoints: endpoints.NewConfig(mockServer.URL),
			}
			cfg.ApplyDefaults()

			limit, err := GetLimit(context.Background(), cfg)

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
				if limit == nil {
					t.Fatal("expected limit response, got nil")
				}
				if limit.MaxSpaceBytes != tc.mockResponse.MaxSpaceBytes {
					t.Errorf("expected MaxSpaceBytes %d, got %d", tc.mockResponse.MaxSpaceBytes, limit.MaxSpaceBytes)
				}
			}
		})
	}
}

func TestGetLimitInvalidJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json response"))
	}))
	defer mockServer.Close()

	cfg := &config.Config{
		Token:     "test-token",
		Endpoints: endpoints.NewConfig(mockServer.URL),
	}
	cfg.ApplyDefaults()

	_, err := GetLimit(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode") {
		t.Errorf("expected error to contain 'failed to decode', got %q", err.Error())
	}
}

func TestGetUsageHTTPClientError(t *testing.T) {
	// Use an invalid URL that will cause the HTTP client to fail
	cfg := &config.Config{
		Token:     "test-token",
		Endpoints: endpoints.NewConfig("http://invalid-host-that-does-not-exist-12345.local"),
	}
	cfg.ApplyDefaults()

	_, err := GetUsage(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error with invalid host, got nil")
	}
	if !strings.Contains(err.Error(), "failed to execute") {
		t.Errorf("expected error to contain 'failed to execute', got %q", err.Error())
	}
}

func TestGetLimitHTTPClientError(t *testing.T) {
	// Use an invalid URL that will cause the HTTP client to fail
	cfg := &config.Config{
		Token:     "test-token",
		Endpoints: endpoints.NewConfig("http://invalid-host-that-does-not-exist-12345.local"),
	}
	cfg.ApplyDefaults()

	_, err := GetLimit(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error with invalid host, got nil")
	}
	if !strings.Contains(err.Error(), "failed to execute") {
		t.Errorf("expected error to contain 'failed to execute', got %q", err.Error())
	}
}
