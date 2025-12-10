package buckets

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
)

func TestTransfer(t *testing.T) {
	t.Run("successful transfer with ETag", func(t *testing.T) {
		testData := []byte("test data to upload")
		hasher := sha1.New()
		hasher.Write(testData)
		expectedETag := hex.EncodeToString(hasher.Sum(nil))

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("expected PUT request, got %s", r.Method)
			}

			if r.Header.Get("Content-Type") != "application/octet-stream" {
				t.Errorf("expected Content-Type application/octet-stream, got %s", r.Header.Get("Content-Type"))
			}

			if r.ContentLength != int64(len(testData)) {
				t.Errorf("expected ContentLength %d, got %d", len(testData), r.ContentLength)
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}
			if !bytes.Equal(body, testData) {
				t.Error("uploaded data does not match expected data")
			}

			w.Header().Set("ETag", `"`+expectedETag+`"`)
			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			HTTPClient: &http.Client{},
		}

		result, err := Transfer(context.Background(), cfg, mockServer.URL, bytes.NewReader(testData), int64(len(testData)))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.ETag != expectedETag {
			t.Errorf("expected ETag %s, got %s", expectedETag, result.ETag)
		}
	})

	t.Run("successful transfer with ETag without quotes", func(t *testing.T) {
		testData := []byte("test data")
		expectedETag := "etag-without-quotes"

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", expectedETag)
			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			HTTPClient: &http.Client{},
		}

		result, err := Transfer(context.Background(), cfg, mockServer.URL, bytes.NewReader(testData), int64(len(testData)))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.ETag != expectedETag {
			t.Errorf("expected ETag %s, got %s", expectedETag, result.ETag)
		}
	})

	t.Run("error - 500 server error", func(t *testing.T) {
		testData := []byte("test data")

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			HTTPClient: &http.Client{},
		}

		_, err := Transfer(context.Background(), cfg, mockServer.URL, bytes.NewReader(testData), int64(len(testData)))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "500") {
			t.Errorf("expected error to contain 500, got %v", err)
		}
	})

	t.Run("error - 401 unauthorized", func(t *testing.T) {
		testData := []byte("test data")

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("unauthorized"))
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			HTTPClient: &http.Client{},
		}

		_, err := Transfer(context.Background(), cfg, mockServer.URL, bytes.NewReader(testData), int64(len(testData)))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "401") {
			t.Errorf("expected error to contain 401, got %v", err)
		}
	})

	t.Run("successful transfer with empty ETag", func(t *testing.T) {
		testData := []byte("test data")

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			HTTPClient: &http.Client{},
		}

		result, err := Transfer(context.Background(), cfg, mockServer.URL, bytes.NewReader(testData), int64(len(testData)))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.ETag != "" {
			t.Errorf("expected empty ETag, got %s", result.ETag)
		}
	})
}
