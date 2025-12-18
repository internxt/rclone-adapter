package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockAccessResponse creates a valid AccessResponse for testing
func mockAccessResponse(newToken string) AccessResponse {
	return AccessResponse{
		Token:    "old-token",
		NewToken: newToken,
	}
}

// TestRefreshToken tests the token refresh functionality
func TestRefreshToken(t *testing.T) {
	testCases := []struct {
		name           string
		token          string
		newToken       string
		mockStatusCode int
		expectError    bool
		errorContains  string
	}{
		{
			name:           "successful token refresh",
			token:          "valid-bearer-token",
			newToken:       "new-refreshed-token",
			mockStatusCode: http.StatusOK,
			expectError:    false,
		},
		{
			name:           "unauthorized - 401",
			token:          "invalid-token",
			mockStatusCode: http.StatusUnauthorized,
			expectError:    true,
			errorContains:  "refresh token failed with status 401",
		},
		{
			name:           "server error - 500",
			token:          "valid-token",
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "refresh token failed with status 500",
		},
		{
			name:           "forbidden - 403",
			token:          "valid-token",
			mockStatusCode: http.StatusForbidden,
			expectError:    true,
			errorContains:  "refresh token failed with status 403",
		},
		{
			name:           "missing newToken in response",
			token:          "valid-token",
			newToken:       "",
			mockStatusCode: http.StatusOK,
			expectError:    true,
			errorContains:  "refresh response missing newToken",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("expected GET request, got %s", r.Method)
				}

				authHeader := r.Header.Get("Authorization")
				expectedAuth := "Bearer " + tc.token
				if authHeader != expectedAuth {
					t.Errorf("expected Authorization header %s, got %s", expectedAuth, authHeader)
				}

				w.WriteHeader(tc.mockStatusCode)
				if tc.mockStatusCode == http.StatusOK {
					json.NewEncoder(w).Encode(mockAccessResponse(tc.newToken))
				} else {
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			cfg := newTestConfig(mockServer.URL, tc.token)

			result, err := RefreshToken(context.Background(), cfg)

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
				if result == nil {
					t.Fatal("expected result, got nil")
				}
				if result.NewToken != tc.newToken {
					t.Errorf("expected NewToken %s, got %s", tc.newToken, result.NewToken)
				}
			}
		})
	}
}

// TestRefreshTokenInvalidJSON tests the token refresh with invalid JSON response
func TestRefreshTokenInvalidJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json response"))
	}))
	defer mockServer.Close()

	cfg := newTestConfig(mockServer.URL, "test-token")

	_, err := RefreshToken(context.Background(), cfg)

	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}

	if !strings.Contains(err.Error(), "failed to parse refresh response") {
		t.Errorf("expected error to contain 'failed to parse refresh response', got %q", err.Error())
	}
}

// TestRefreshTokenRequestFormat verifies the exact format of the request
func TestRefreshTokenRequestFormat(t *testing.T) {
	requestReceived := false
	var capturedMethod string
	var capturedAuthHeader string

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		capturedMethod = r.Method
		capturedAuthHeader = r.Header.Get("Authorization")

		response := AccessResponse{
			NewToken: "new-token",
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := newTestConfig(mockServer.URL, "my-test-token")

	_, err := RefreshToken(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !requestReceived {
		t.Fatal("request was not received by mock server")
	}

	if capturedMethod != http.MethodGet {
		t.Errorf("expected GET method, got %s", capturedMethod)
	}

	expectedAuth := "Bearer my-test-token"
	if capturedAuthHeader != expectedAuth {
		t.Errorf("expected Authorization header %s, got %s", expectedAuth, capturedAuthHeader)
	}
}
