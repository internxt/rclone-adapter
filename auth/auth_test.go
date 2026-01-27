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

// mockLoginResponse creates a valid LoginResponse for testing
func mockLoginResponse() LoginResponse {
	return LoginResponse{
		HasKeys:      true,
		SKey:         "test-skey",
		TFA:          false,
		HasKyberKeys: true,
		HasEccKeys:   true,
	}
}

// TestLogin tests the login functionality
func TestLogin(t *testing.T) {
	testCases := []struct {
		name           string
		email          string
		mockStatusCode int
		expectError    bool
		errorContains  string
		mockResponse   LoginResponse
	}{
		{
			name:           "successful login",
			email:          "test@example.com",
			mockStatusCode: http.StatusOK,
			expectError:    false,
			mockResponse:   mockLoginResponse(),
		},
		{
			name:           "successful login with 2FA enabled",
			email:          "test@example.com",
			mockStatusCode: http.StatusOK,
			expectError:    false,
			mockResponse: LoginResponse{
				HasKeys:      true,
				SKey:         "test-skey-2fa",
				TFA:          true,
				HasKyberKeys: true,
				HasEccKeys:   true,
			},
		},
		{
			name:           "bad request - 400",
			email:          "invalid-email",
			mockStatusCode: http.StatusBadRequest,
			expectError:    true,
			errorContains:  "status 400",
		},
		{
			name:           "server error - 500",
			email:          "test@example.com",
			mockStatusCode: http.StatusInternalServerError,
			expectError:    true,
			errorContains:  "status 500",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("expected POST request, got %s", r.Method)
				}

				contentType := r.Header.Get("Content-Type")
				if contentType != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", contentType)
				}

				var reqBody LoginRequest
				if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				if reqBody.Email != tc.email {
					t.Errorf("expected email %s, got %s", tc.email, reqBody.Email)
				}

				w.WriteHeader(tc.mockStatusCode)
				if tc.mockStatusCode == http.StatusOK {
					json.NewEncoder(w).Encode(tc.mockResponse)
				} else {
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			cfg := newTestConfig(mockServer.URL, "")

			result, err := Login(context.Background(), cfg, tc.email)

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
				if result.HasKeys != tc.mockResponse.HasKeys {
					t.Errorf("expected HasKeys %v, got %v", tc.mockResponse.HasKeys, result.HasKeys)
				}
				if result.SKey != tc.mockResponse.SKey {
					t.Errorf("expected SKey %s, got %s", tc.mockResponse.SKey, result.SKey)
				}
				if result.TFA != tc.mockResponse.TFA {
					t.Errorf("expected TFA %v, got %v", tc.mockResponse.TFA, result.TFA)
				}
				if result.HasKyberKeys != tc.mockResponse.HasKyberKeys {
					t.Errorf("expected HasKyberKeys %v, got %v", tc.mockResponse.HasKyberKeys, result.HasKyberKeys)
				}
				if result.HasEccKeys != tc.mockResponse.HasEccKeys {
					t.Errorf("expected HasEccKeys %v, got %v", tc.mockResponse.HasEccKeys, result.HasEccKeys)
				}
			}
		})
	}
}

// TestLoginInvalidJSON tests the login with invalid JSON response
func TestLoginInvalidJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json response"))
	}))
	defer mockServer.Close()

	cfg := newTestConfig(mockServer.URL, "")

	_, err := Login(context.Background(), cfg, "test@example.com")

	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}

	if !strings.Contains(err.Error(), "failed to parse login response") {
		t.Errorf("expected error to contain 'failed to parse login response', got %q", err.Error())
	}
}

// TestLoginRequestFormat verifies the exact format of the request
func TestLoginRequestFormat(t *testing.T) {
	requestReceived := false
	var capturedMethod string
	var capturedContentType string
	var capturedEmail string

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		capturedMethod = r.Method
		capturedContentType = r.Header.Get("Content-Type")

		var reqBody LoginRequest
		json.NewDecoder(r.Body).Decode(&reqBody)
		capturedEmail = reqBody.Email

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(mockLoginResponse())
	}))
	defer mockServer.Close()

	cfg := newTestConfig(mockServer.URL, "")

	_, err := Login(context.Background(), cfg, "test@example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !requestReceived {
		t.Fatal("request was not received by mock server")
	}

	if capturedMethod != http.MethodPost {
		t.Errorf("expected POST method, got %s", capturedMethod)
	}

	if capturedContentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", capturedContentType)
	}

	if capturedEmail != "test@example.com" {
		t.Errorf("expected email test@example.com, got %s", capturedEmail)
	}
}
