package users

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/internxt/rclone-adapter/schema"
	"github.com/stretchr/testify/assert"
)

func TestGetUsage(t *testing.T) {
	testCases := []struct {
		name           string
		mockResponse   schema.GetUserUsageDto
		mockStatusCode int
		errorContains  string
	}{
		{
			name: "successful usage retrieval",
			mockResponse: schema.GetUserUsageDto{
				Drive: 1024 * 1024 * 1024, // 1 GB
			},
			mockStatusCode: http.StatusOK,
		},
		{
			name: "zero usage",
			mockResponse: schema.GetUserUsageDto{
				Drive: 0,
			},
			mockStatusCode: http.StatusOK,
		},
		{
			name:           "unauthorized - 401",
			mockStatusCode: http.StatusUnauthorized,
			errorContains:  "401",
		},
		{
			name:           "server error - 500",
			mockStatusCode: http.StatusInternalServerError,
			errorContains:  "500",
		},
		{
			name:           "forbidden - 403",
			mockStatusCode: http.StatusForbidden,
			errorContains:  "403",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, r.Method, "GET")
				assert.Equal(t, r.Header.Get("Authorization"), "Bearer token")
				assert.Contains(t, r.URL.Path, "/usage")

				if tc.mockStatusCode == http.StatusOK {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.mockStatusCode)
					json.NewEncoder(w).Encode(tc.mockResponse)
				} else {
					w.WriteHeader(tc.mockStatusCode)
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			client, _ := schema.NewOpenapiClient(mockServer.URL, "token")
			usage, err := GetUsage(context.Background(), client)

			if tc.errorContains != "" {
				assert.ErrorContains(t, err, tc.errorContains)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, usage)
				assert.Equal(t, tc.mockResponse.Drive, usage.Drive)
			}
		})
	}
}

func TestGetUsageInvalidJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json response"))
	}))
	defer mockServer.Close()

	client, _ := schema.NewOpenapiClient(mockServer.URL, "token")
	_, err := GetUsage(context.Background(), client)

	assert.ErrorContains(t, err, "failed to parse response")
}

func TestGetLimit(t *testing.T) {
	testCases := []struct {
		name           string
		mockResponse   schema.GetUserLimitDto
		mockStatusCode int
		errorContains  string
	}{
		{
			name: "successful limit retrieval",
			mockResponse: schema.GetUserLimitDto{
				MaxSpaceBytes: 10 * 1024 * 1024 * 1024, // 10 GB
			},
			mockStatusCode: http.StatusOK,
		},
		{
			name: "zero limit",
			mockResponse: schema.GetUserLimitDto{
				MaxSpaceBytes: 0,
			},
			mockStatusCode: http.StatusOK,
		},
		{
			name:           "unauthorized - 401",
			mockStatusCode: http.StatusUnauthorized,
			errorContains:  "401",
		},
		{
			name:           "server error - 500",
			mockStatusCode: http.StatusInternalServerError,
			errorContains:  "500",
		},
		{
			name:           "not found - 404",
			mockStatusCode: http.StatusNotFound,
			errorContains:  "404",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, r.Method, "GET")
				assert.Equal(t, r.Header.Get("Authorization"), "Bearer token")
				assert.Contains(t, r.URL.Path, "/limit")

				if tc.mockStatusCode == http.StatusOK {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.mockStatusCode)
					json.NewEncoder(w).Encode(tc.mockResponse)
				} else {
					w.WriteHeader(tc.mockStatusCode)
					w.Write([]byte("error message"))
				}
			}))
			defer mockServer.Close()

			client, _ := schema.NewOpenapiClient(mockServer.URL, "token")
			limit, err := GetLimit(context.Background(), client)

			if tc.errorContains != "" {
				assert.ErrorContains(t, err, tc.errorContains)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, limit)
				assert.Equal(t, tc.mockResponse.MaxSpaceBytes, limit.MaxSpaceBytes)
			}
		})
	}
}

func TestGetLimitInvalidJSON(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json response"))
	}))
	defer mockServer.Close()

	client, _ := schema.NewOpenapiClient(mockServer.URL, "token")
	_, err := GetLimit(context.Background(), client)

	assert.ErrorContains(t, err, "failed to parse response")
}

func TestGetUsageHTTPClientError(t *testing.T) {
	client, _ := schema.NewOpenapiClient("http://invalid-host-that-does-not-exist", "token")
	_, err := GetUsage(context.Background(), client)
	assert.ErrorContains(t, err, "no such host")
}

func TestGetLimitHTTPClientError(t *testing.T) {
	client, _ := schema.NewOpenapiClient("http://invalid-host-that-does-not-exist", "token")
	_, err := GetLimit(context.Background(), client)
	assert.ErrorContains(t, err, "no such host")
}
