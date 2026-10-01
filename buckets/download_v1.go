package buckets

import (
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"sort"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/errors"
)

const (
	mirrorsPageSize        = 3
	maxPointerReplacements = 6
)

// Mirror is a legacy (v1) shard pointer returned by GET /buckets/{bucketID}/files/{fileID}.
type Mirror struct {
	Index  int    `json:"index"`
	Hash   string `json:"hash"`
	Size   int64  `json:"size"`
	Parity bool   `json:"parity"`
	URL    string `json:"url"`
}

// isLegacyFile reports whether info describes a v1 file, which the bridge
// returns without shards and without version 2.
func isLegacyFile(info *BucketFileInfo) bool {
	return info.Version != 2 && len(info.Shards) == 0
}

// GetFileMirrors returns the data shard pointers of a v1 file sorted by index.
// Pointers whose farmer did not respond have an empty URL.
func GetFileMirrors(ctx context.Context, cfg *config.Config, bucketID, fileID string) ([]Mirror, error) {
	var mirrors []Mirror
	for skip := 0; ; skip += mirrorsPageSize {
		page, err := getMirrorsPage(ctx, cfg, bucketID, fileID, mirrorsPageSize, skip)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		for _, m := range page {
			if !m.Parity {
				mirrors = append(mirrors, m)
			}
		}
	}
	sort.Slice(mirrors, func(i, j int) bool { return mirrors[i].Index < mirrors[j].Index })
	return mirrors, nil
}

// checkMirrors verifies that the data shards are contiguous from index 0 and
// cover size bytes, so a missing pointer fails instead of yielding a truncated
// or misdecrypted file.
func checkMirrors(mirrors []Mirror, size int64, fileID string) error {
	var total int64
	for i, m := range mirrors {
		if m.Index != i {
			return fmt.Errorf("missing shard %d of file %s", i, fileID)
		}
		total += m.Size
	}
	if total < size {
		return fmt.Errorf("shards of file %s cover %d of %d bytes", fileID, total, size)
	}
	return nil
}

// replacePointer asks the bridge for a new pointer to the shard at index,
// retrying until one comes back with a farmer URL.
func replacePointer(ctx context.Context, cfg *config.Config, bucketID, fileID string, index int) (Mirror, error) {
	for range maxPointerReplacements {
		page, err := getMirrorsPage(ctx, cfg, bucketID, fileID, 1, index)
		if err != nil {
			return Mirror{}, err
		}
		if len(page) > 0 && page[0].URL != "" {
			return page[0], nil
		}
	}
	return Mirror{}, fmt.Errorf("missing pointer for shard %d of file %s", index, fileID)
}

// getMirrorsPage fetches up to limit shard pointers of a v1 file starting at
// index skip.
func getMirrorsPage(ctx context.Context, cfg *config.Config, bucketID, fileID string, limit, skip int) ([]Mirror, error) {
	url := cfg.Endpoints.Network().FileMirrors(bucketID, fileID, limit, skip)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create get file mirrors request: %w", err)
	}
	req.Header.Set("Authorization", cfg.BasicAuthHeader)

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute get file mirrors request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.NewHTTPError(resp, "get file mirrors")
	}

	var mirrors []Mirror
	if err := json.NewDecoder(resp.Body).Decode(&mirrors); err != nil {
		return nil, fmt.Errorf("failed to decode file mirrors response: %w", err)
	}
	return mirrors, nil
}

// downloadFileV1 downloads and decrypts a v1 file into destPath, removing the
// partial file on failure.
func downloadFileV1(ctx context.Context, cfg *config.Config, info *BucketFileInfo, fileID, destPath string) error {
	src, err := downloadFileStreamV1(ctx, cfg, info, fileID, "")
	if err != nil {
		return err
	}
	defer src.Close()

	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", destPath, err)
	}
	if _, err := io.Copy(out, src); err != nil {
		out.Close()
		os.Remove(destPath)
		return fmt.Errorf("failed to write decrypted data to file: %w", err)
	}
	return out.Close()
}

// downloadFileStreamV1 streams the decrypted contents of a v1 file. Shards are
// fetched whole, starting at the first one that covers the requested range.
func downloadFileStreamV1(ctx context.Context, cfg *config.Config, info *BucketFileInfo, fileID, rangeValue string) (io.ReadCloser, error) {
	start, end := int64(0), info.Size-1
	if rangeValue != "" {
		s, e, err := getStartByteAndEndByte(rangeValue)
		if err != nil {
			return nil, fmt.Errorf("invalid range: %w", err)
		}
		start = int64(s)
		if e >= 0 {
			end = min(int64(e), end)
		}
	}
	if start > end {
		return nil, fmt.Errorf("range %q not satisfiable for file %s of size %d", rangeValue, fileID, info.Size)
	}

	mirrors, err := GetFileMirrors(ctx, cfg, cfg.Bucket, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get file mirrors: %w", err)
	}
	if err := checkMirrors(mirrors, info.Size, fileID); err != nil {
		return nil, err
	}
	mirrors, shardStart := shardsFrom(mirrors, start)
	if len(mirrors) == 0 {
		return nil, fmt.Errorf("no shard covers offset %d of file %s", start, fileID)
	}

	key, iv, err := GenerateFileKey(cfg.Mnemonic, cfg.Bucket, info.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to generate file key: %w", err)
	}
	stream, err := newCTRStreamAt(key, iv, shardStart)
	if err != nil {
		return nil, err
	}

	shards := &shardReader{ctx: ctx, cfg: cfg, mirrors: mirrors, fileID: fileID}
	if !cfg.SkipHashValidation {
		shards.hasher = sha256.New()
	}
	plain := cipher.StreamReader{S: stream, R: shards}
	if _, err := io.CopyN(io.Discard, plain, start-shardStart); err != nil {
		shards.Close()
		return nil, fmt.Errorf("failed to skip to range start: %w", err)
	}

	return struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(plain, end-start+1), Closer: shards}, nil
}

// shardsFrom returns the shards starting at the one that contains offset,
// along with that shard's offset within the file.
func shardsFrom(mirrors []Mirror, offset int64) ([]Mirror, int64) {
	var pos int64
	for i, m := range mirrors {
		if offset < pos+m.Size {
			return mirrors[i:], pos
		}
		pos += m.Size
	}
	return nil, pos
}

// shardReader concatenates the encrypted shards of a v1 file, fetching them
// sequentially and validating each shard's hash once it has been fully read.
// A nil hasher skips validation.
type shardReader struct {
	ctx     context.Context
	cfg     *config.Config
	mirrors []Mirror
	fileID  string
	hasher  hash.Hash

	body      io.ReadCloser
	remaining int64
}

// Read reads the next encrypted bytes, opening the following shard as each one
// is exhausted.
func (r *shardReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if r.body == nil {
			if len(r.mirrors) == 0 {
				return 0, io.EOF
			}
			if err := r.open(); err != nil {
				return 0, err
			}
		}

		if int64(len(p)) > r.remaining {
			p = p[:r.remaining]
		}
		n, err := r.body.Read(p)
		r.remaining -= int64(n)
		if r.hasher != nil {
			r.hasher.Write(p[:n])
		}

		if r.remaining == 0 {
			if err := r.finishShard(); err != nil {
				return n, err
			}
			err = nil
		} else if err == io.EOF {
			err = fmt.Errorf("shard %d of file %s: %w", r.mirrors[0].Index, r.fileID, io.ErrUnexpectedEOF)
		}

		if n > 0 || err != nil {
			return n, err
		}
	}
}

// open starts the download of the next shard, replacing its pointer first if
// the farmer did not respond.
func (r *shardReader) open() error {
	m := r.mirrors[0]
	if m.URL == "" {
		replacement, err := replacePointer(r.ctx, r.cfg, r.cfg.Bucket, r.fileID, m.Index)
		if err != nil {
			return err
		}
		m.URL = replacement.URL
	}

	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, m.URL, nil)
	if err != nil {
		return fmt.Errorf("failed to create shard %d download request: %w", m.Index, err)
	}
	resp, err := r.cfg.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download shard %d: %w", m.Index, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		httpErr := errors.NewHTTPError(resp, "v1 shard download")
		resp.Body.Close()
		return httpErr
	}

	r.body = resp.Body
	r.remaining = m.Size
	if r.hasher != nil {
		r.hasher.Reset()
	}
	return nil
}

// finishShard closes the current shard, validates its hash and advances to the
// next one.
func (r *shardReader) finishShard() error {
	m := r.mirrors[0]
	r.mirrors = r.mirrors[1:]
	r.Close()

	if r.hasher == nil {
		return nil
	}
	if got := ComputeFileHash(r.hasher.Sum(nil)); got != m.Hash {
		return fmt.Errorf("hash mismatch for shard %d of file %s: expected %s, got %s", m.Index, r.fileID, m.Hash, got)
	}
	return nil
}

// Close closes the shard being downloaded, if any.
func (r *shardReader) Close() error {
	if r.body == nil {
		return nil
	}
	err := r.body.Close()
	r.body = nil
	return err
}
