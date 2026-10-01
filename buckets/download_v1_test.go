package buckets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
)

// v1Fixture serves a legacy file split into shards of uneven, non block-aligned sizes.
type v1Fixture struct {
	plain  []byte
	shards [][]byte
	hashes []string

	// deadPointers is how many times the mirror for shard 1 is returned without a farmer.
	deadPointers int
	// corruptShard flips a byte of the shard with this index when served, or -1.
	corruptShard int
	// missingShard is left out of mirror listings, or -1.
	missingShard int

	shardRequests int
}

func newV1Fixture(t *testing.T) *v1Fixture {
	t.Helper()
	plain := make([]byte, 100)
	for i := range plain {
		plain[i] = byte(i)
	}

	key, iv, err := GenerateFileKey(TestMnemonic, TestBucket1, TestIndex)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	enc, err := EncryptReader(bytes.NewReader(plain), key, iv)
	if err != nil {
		t.Fatalf("failed to create encrypt reader: %v", err)
	}
	cipherText, err := io.ReadAll(enc)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	f := &v1Fixture{plain: plain, corruptShard: -1, missingShard: -1}
	for _, bounds := range [][2]int{{0, 40}, {40, 77}, {77, 100}} {
		shard := cipherText[bounds[0]:bounds[1]]
		sum := sha256.Sum256(shard)
		f.shards = append(f.shards, shard)
		f.hashes = append(f.hashes, ComputeFileHash(sum[:]))
	}
	return f
}

func (f *v1Fixture) serve(t *testing.T) *config.Config {
	t.Helper()
	var server *httptest.Server
	mux := http.NewServeMux()

	mux.HandleFunc("GET /network/buckets/{bucket}/files/{file}/info", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"bucket": TestBucket1,
			"index":  TestIndex,
			"size":   len(f.plain),
			"id":     r.PathValue("file"),
			"frame":  "frame-id",
		})
	})

	mux.HandleFunc("GET /network/buckets/{bucket}/files/{file}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != TestBasicAuth {
			t.Errorf("missing auth header on mirrors request")
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		skip, _ := strconv.Atoi(r.URL.Query().Get("skip"))

		mirrors := []Mirror{}
		for i := skip; i < skip+limit; i++ {
			switch {
			case i == f.missingShard:
			case i < len(f.shards):
				m := Mirror{
					Index: i,
					Hash:  f.hashes[i],
					Size:  int64(len(f.shards[i])),
					URL:   fmt.Sprintf("%s/shard/%d", server.URL, i),
				}
				if i == 1 && f.deadPointers > 0 {
					f.deadPointers--
					m.URL = ""
				}
				mirrors = append(mirrors, m)
			case i == len(f.shards):
				mirrors = append(mirrors, Mirror{Index: i, Parity: true, Size: 40})
			}
		}
		json.NewEncoder(w).Encode(mirrors)
	})

	mux.HandleFunc("GET /shard/{index}", func(w http.ResponseWriter, r *http.Request) {
		f.shardRequests++
		i, _ := strconv.Atoi(r.PathValue("index"))
		shard := bytes.Clone(f.shards[i])
		if i == f.corruptShard {
			shard[0] ^= 0xFF
		}
		w.Write(shard)
	})

	server = httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return newTestConfig(server.URL)
}

func TestDownloadFileStreamV1_Full(t *testing.T) {
	f := newV1Fixture(t)
	f.deadPointers = 2
	cfg := f.serve(t)

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !bytes.Equal(got, f.plain) {
		t.Errorf("content mismatch:\nwant: %v\ngot:  %v", f.plain, got)
	}
}

func TestDownloadFileStreamV1_Range(t *testing.T) {
	tests := []struct {
		name       string
		rangeValue string
		start, end int
		shards     int
	}{
		{"within first shard", "bytes=3-20", 3, 20, 1},
		{"across shard boundary", "bytes=35-50", 35, 50, 2},
		{"starts in second shard", "bytes=45-", 45, 99, 2},
		{"starts at shard boundary", "bytes=77-80", 77, 80, 1},
		{"end clamped to size", "bytes=90-500", 90, 99, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newV1Fixture(t)
			cfg := f.serve(t)

			stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID, tt.rangeValue)
			if err != nil {
				t.Fatalf("DownloadFileStream failed: %v", err)
			}
			defer stream.Close()

			got, err := io.ReadAll(stream)
			if err != nil {
				t.Fatalf("failed to read stream: %v", err)
			}
			if want := f.plain[tt.start : tt.end+1]; !bytes.Equal(got, want) {
				t.Errorf("content mismatch:\nwant: %v\ngot:  %v", want, got)
			}
			if f.shardRequests != tt.shards {
				t.Errorf("expected %d shard requests, got %d", tt.shards, f.shardRequests)
			}
		})
	}
}

func TestDownloadFileStreamV1_RangeNotSatisfiable(t *testing.T) {
	f := newV1Fixture(t)
	cfg := f.serve(t)

	_, err := DownloadFileStream(context.Background(), cfg, testFileUUID, "bytes=100-")
	if err == nil || !strings.Contains(err.Error(), "not satisfiable") {
		t.Fatalf("expected range not satisfiable error, got %v", err)
	}
}

func TestDownloadFileStreamV1_HashMismatch(t *testing.T) {
	f := newV1Fixture(t)
	f.corruptShard = 1
	cfg := f.serve(t)

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}
	defer stream.Close()

	_, err = io.ReadAll(stream)
	if err == nil || !strings.Contains(err.Error(), "hash mismatch for shard 1") {
		t.Fatalf("expected hash mismatch error, got %v", err)
	}
}

func TestDownloadFileStreamV1_SkipHashValidation(t *testing.T) {
	f := newV1Fixture(t)
	f.corruptShard = 1
	cfg := f.serve(t)
	cfg.SkipHashValidation = true

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}
	defer stream.Close()

	if _, err := io.ReadAll(stream); err != nil {
		t.Fatalf("expected no error with hash validation skipped, got %v", err)
	}
}

func TestDownloadFileStreamV1_MissingPointer(t *testing.T) {
	f := newV1Fixture(t)
	f.deadPointers = 1 + maxPointerReplacements
	cfg := f.serve(t)

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}
	defer stream.Close()

	_, err = io.ReadAll(stream)
	if err == nil || !strings.Contains(err.Error(), "missing pointer for shard 1") {
		t.Fatalf("expected missing pointer error, got %v", err)
	}
}

func TestDownloadFileStreamV1_MissingShard(t *testing.T) {
	tests := []struct {
		name    string
		missing int
		wantErr string
	}{
		{"gap in the middle", 1, "missing shard 1"},
		{"last shard", 2, "cover 77 of 100 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newV1Fixture(t)
			f.missingShard = tt.missing
			cfg := f.serve(t)

			_, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
			if f.shardRequests != 0 {
				t.Errorf("expected no shard requests, got %d", f.shardRequests)
			}
		})
	}
}

func TestDownloadFileStreamV1_EmptyRead(t *testing.T) {
	f := newV1Fixture(t)
	cfg := f.serve(t)

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}
	defer stream.Close()

	if n, err := stream.Read(nil); n != 0 || err != nil {
		t.Fatalf("expected 0, nil from empty read, got %d, %v", n, err)
	}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}
	if !bytes.Equal(got, f.plain) {
		t.Errorf("content mismatch:\nwant: %v\ngot:  %v", f.plain, got)
	}
}

func TestDownloadFileV1(t *testing.T) {
	t.Run("writes decrypted file", func(t *testing.T) {
		f := newV1Fixture(t)
		cfg := f.serve(t)
		dest := filepath.Join(t.TempDir(), "file")

		if err := DownloadFile(context.Background(), cfg, testFileUUID, dest); err != nil {
			t.Fatalf("DownloadFile failed: %v", err)
		}
		got, err := os.ReadFile(dest)
		if err != nil {
			t.Fatalf("failed to read downloaded file: %v", err)
		}
		if !bytes.Equal(got, f.plain) {
			t.Errorf("content mismatch:\nwant: %v\ngot:  %v", f.plain, got)
		}
	})

	t.Run("removes file on hash mismatch", func(t *testing.T) {
		f := newV1Fixture(t)
		f.corruptShard = 2
		cfg := f.serve(t)
		dest := filepath.Join(t.TempDir(), "file")

		err := DownloadFile(context.Background(), cfg, testFileUUID, dest)
		if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
			t.Fatalf("expected hash mismatch error, got %v", err)
		}
		if _, err := os.Stat(dest); !os.IsNotExist(err) {
			t.Error("corrupted file should have been removed")
		}
	})
}
