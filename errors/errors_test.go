package errors

import (
	"net/http"
	"testing"
	"time"
)

func newHTTPResponse(statusCode int, headers map[string]string) *http.Response {
	h := make(http.Header)
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode: statusCode,
		Header:     h,
	}
}

func TestRetryAfter_Seconds(t *testing.T) {
	e := &HTTPError{Response: newHTTPResponse(429, map[string]string{
		"Retry-After": "30",
	})}

	got := e.RetryAfter()
	want := 30 * time.Second
	if got != want {
		t.Errorf("RetryAfter() = %v, want %v", got, want)
	}
}

func TestRetryAfter_HTTPDate(t *testing.T) {
	future := time.Now().Add(45 * time.Second)
	dateStr := future.UTC().Format(http.TimeFormat)

	e := &HTTPError{Response: newHTTPResponse(429, map[string]string{
		"Retry-After": dateStr,
	})}

	got := e.RetryAfter()
	if got < 43*time.Second || got > 47*time.Second {
		t.Errorf("RetryAfter() = %v, want ~45s", got)
	}
}

func TestRetryAfter_NoHeader(t *testing.T) {
	e := &HTTPError{Response: newHTTPResponse(429, nil)}

	got := e.RetryAfter()
	if got != 0 {
		t.Errorf("RetryAfter() = %v, want 0", got)
	}
}

func TestRetryAfter_InvalidValue(t *testing.T) {
	e := &HTTPError{Response: newHTTPResponse(429, map[string]string{
		"Retry-After": "not-a-number",
	})}

	got := e.RetryAfter()
	if got != 0 {
		t.Errorf("RetryAfter() = %v, want 0 for unparseable value", got)
	}
}

func TestRetryAfter_ZeroSeconds(t *testing.T) {
	e := &HTTPError{Response: newHTTPResponse(429, map[string]string{
		"Retry-After": "0",
	})}

	got := e.RetryAfter()
	if got != 0 {
		t.Errorf("RetryAfter() = %v, want 0", got)
	}
}

func TestRetryAfter_PastHTTPDate(t *testing.T) {
	past := time.Now().Add(-10 * time.Second)
	dateStr := past.UTC().Format(http.TimeFormat)

	e := &HTTPError{Response: newHTTPResponse(429, map[string]string{
		"Retry-After": dateStr,
	})}

	got := e.RetryAfter()
	if got != 0 {
		t.Errorf("RetryAfter() = %v, want 0 for past date", got)
	}
}

func TestTemporary(t *testing.T) {
	tests := []struct {
		code int
		want bool
	}{
		{200, false},
		{400, false},
		{401, false},
		{403, false},
		{404, false},
		{408, true},
		{429, true},
		{500, true},
		{502, true},
		{503, true},
	}

	for _, tc := range tests {
		e := &HTTPError{Response: newHTTPResponse(tc.code, nil)}
		if got := e.Temporary(); got != tc.want {
			t.Errorf("Temporary() for status %d = %v, want %v", tc.code, got, tc.want)
		}
	}
}
