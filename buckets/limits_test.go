package buckets

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkerrors "github.com/internxt/rclone-adapter/errors"
)

func TestGetFileLimits_DecodesPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/drive/files/limits") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+TestToken {
			t.Errorf("missing or wrong auth header: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"versioning":{"enabled":true,"retentionDays":30},"maxUploadFileSize":524288000}`))
	}))
	defer server.Close()

	cfg := newTestConfig(server.URL)

	resp, err := GetFileLimits(context.Background(), cfg)
	if err != nil {
		t.Fatalf("GetFileLimits: %v", err)
	}
	if resp.MaxUploadFileSize != 524288000 {
		t.Errorf("MaxUploadFileSize = %d, want 524288000", resp.MaxUploadFileSize)
	}
	if len(resp.Versioning) == 0 {
		t.Error("expected Versioning to be captured as raw JSON")
	}
}

func TestGetFileLimits_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	defer server.Close()

	cfg := newTestConfig(server.URL)

	_, err := GetFileLimits(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error on 500")
	}
	var httpErr *sdkerrors.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected HTTPError, got %T: %v", err, err)
	}
	if httpErr.StatusCode() != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", httpErr.StatusCode())
	}
}

func TestCheckUploadSize(t *testing.T) {
	const maxSize int64 = 1024

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(LimitsResponse{MaxUploadFileSize: maxSize})
	}))
	defer server.Close()

	cases := []struct {
		name      string
		plainSize int64
		wantErr   bool
	}{
		{"under limit", maxSize - 1, false},
		{"at limit", maxSize, false},
		{"over limit", maxSize + 1, true},
		{"unknown size skips check", -1, false},
		{"zero size", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newTestConfig(server.URL)
			err := checkUploadSize(context.Background(), cfg, tc.plainSize)
			if tc.wantErr {
				var tooLarge *sdkerrors.FileTooLargeError
				if !errors.As(err, &tooLarge) {
					t.Fatalf("expected FileTooLargeError, got %v", err)
				}
				if tooLarge.Size != tc.plainSize || tooLarge.MaxSize != maxSize {
					t.Errorf("FileTooLargeError = {Size:%d MaxSize:%d}, want {Size:%d MaxSize:%d}",
						tooLarge.Size, tooLarge.MaxSize, tc.plainSize, maxSize)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCheckUploadSize_FailOpenOnFetchError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := newTestConfig(server.URL)

	// Even with a "huge" upload, a fetch error means the SDK cannot
	// determine the limit and must fail-open — the server still enforces.
	if err := checkUploadSize(context.Background(), cfg, 10*1024*1024*1024); err != nil {
		t.Fatalf("expected fail-open (nil error), got: %v", err)
	}
}

func TestCheckUploadSize_ZeroOrNegativeMaxSkipsCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(LimitsResponse{MaxUploadFileSize: 0})
	}))
	defer server.Close()

	cfg := newTestConfig(server.URL)

	if err := checkUploadSize(context.Background(), cfg, 1<<30); err != nil {
		t.Fatalf("expected nil error for unlimited account, got %v", err)
	}
}

func TestFileLimitsCache_HitsAreNotRefetched(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(LimitsResponse{MaxUploadFileSize: 100})
	}))
	defer server.Close()

	cfg := newTestConfig(server.URL)

	for range 5 {
		if err := checkUploadSize(context.Background(), cfg, 50); err != nil {
			t.Fatalf("checkUploadSize: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("limits endpoint called %d times, want 1", got)
	}
}

func TestFileLimitsCache_ConcurrentCallersSingleFetch(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(LimitsResponse{MaxUploadFileSize: 100})
	}))
	defer server.Close()

	cfg := newTestConfig(server.URL)

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = checkUploadSize(context.Background(), cfg, 50)
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("limits endpoint called %d times under concurrency, want 1", got)
	}
}

func TestFileLimitsCache_ExpiryTriggersRefetch(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(LimitsResponse{MaxUploadFileSize: 100})
	}))
	defer server.Close()

	cfg := newTestConfig(server.URL)

	if err := checkUploadSize(context.Background(), cfg, 50); err != nil {
		t.Fatalf("checkUploadSize: %v", err)
	}
	cfg.FileLimits.Invalidate()
	if err := checkUploadSize(context.Background(), cfg, 50); err != nil {
		t.Fatalf("checkUploadSize after invalidate: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("limits endpoint called %d times, want 2 (one per fetch)", got)
	}
}
