package buckets

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/internxt/rclone-adapter/config"
)

// multipartUploadState holds the state for a single multipart upload session
type multipartUploadState struct {
	cfg            *config.Config
	plainIndex     string
	encIndex       string
	fileKey        []byte
	iv             []byte
	cipher         cipher.Stream
	totalSize      int64
	chunkSize      int64
	numParts       int64
	startResp      *StartUploadResp
	maxConcurrency int
	uploadId       string
	uuid           string
}

// uploadResult holds the result of a single chunk upload
type uploadResult struct {
	index int
	etag  string
	err   error
}

// newMultipartUploadState initializes encryption parameters and cipher for multipart upload
func newMultipartUploadState(cfg *config.Config, plainSize int64) (*multipartUploadState, error) {
	var ph [32]byte
	if _, err := rand.Read(ph[:]); err != nil {
		return nil, fmt.Errorf("cannot generate random index: %w", err)
	}

	plainIndex := hex.EncodeToString(ph[:])

	fileKey, iv, err := GenerateFileKey(cfg.Mnemonic, cfg.Bucket, plainIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to generate file key: %w", err)
	}

	cipherStream, err := NewAES256CTRCipher(fileKey, iv)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	chunkSize := int64(config.DefaultChunkSize)
	numParts := (plainSize + chunkSize - 1) / chunkSize

	return &multipartUploadState{
		cfg:            cfg,
		plainIndex:     plainIndex,
		encIndex:       plainIndex,
		fileKey:        fileKey,
		iv:             iv,
		cipher:         cipherStream,
		totalSize:      plainSize,
		chunkSize:      chunkSize,
		numParts:       numParts,
		maxConcurrency: config.DefaultMaxConcurrency,
	}, nil
}

// executeMultipartUpload orchestrates the entire multipart upload process
func (s *multipartUploadState) executeMultipartUpload(reader io.Reader) (*MultipartShard, error) {
	specs := []UploadPartSpec{{Index: 0, Size: s.totalSize}}

	var err error
	s.startResp, err = StartUploadMultipart(s.cfg, s.cfg.Bucket, specs, int(s.numParts))
	if err != nil {
		return nil, fmt.Errorf("failed to start multipart upload: %w", err)
	}

	if len(s.startResp.Uploads) != 1 {
		return nil, fmt.Errorf("expected 1 upload entry, got %d", len(s.startResp.Uploads))
	}

	uploadInfo := s.startResp.Uploads[0]
	if len(uploadInfo.URLs) != int(s.numParts) {
		return nil, fmt.Errorf("expected %d URLs, got %d", s.numParts, len(uploadInfo.URLs))
	}

	s.uploadId = uploadInfo.UploadId
	s.uuid = uploadInfo.UUID

	encryptedChunks, err := s.encryptAllChunks(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt chunks: %w", err)
	}

	completedParts, overallHash, err := s.uploadConcurrently(encryptedChunks)
	if err != nil {
		return nil, fmt.Errorf("failed to upload chunks: %w", err)
	}

	return &MultipartShard{
		UUID:     s.uuid,
		Hash:     overallHash,
		UploadId: s.uploadId,
		Parts:    completedParts,
	}, nil
}

// encryptAllChunks reads and encrypts all chunks sequentially using the single cipher instance
func (s *multipartUploadState) encryptAllChunks(reader io.Reader) ([][]byte, error) {
	encryptedChunks := make([][]byte, s.numParts)

	for i := int64(0); i < s.numParts; i++ {
		chunkSize := s.chunkSize
		if i == s.numParts-1 {
			chunkSize = s.totalSize - (i * s.chunkSize)
		}

		plainChunk := make([]byte, chunkSize)
		n, err := io.ReadFull(reader, plainChunk)
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("failed to read chunk %d: %w", i, err)
		}
		plainChunk = plainChunk[:n]

		encryptedChunk := make([]byte, len(plainChunk))
		s.cipher.XORKeyStream(encryptedChunk, plainChunk)

		encryptedChunks[i] = encryptedChunk
	}

	return encryptedChunks, nil
}

// uploadConcurrently uploads all encrypted chunks with controlled concurrency
// Returns completed parts, overall hash, and any error
func (s *multipartUploadState) uploadConcurrently(encryptedChunks [][]byte) ([]CompletedPart, string, error) {
	semaphore := make(chan struct{}, s.maxConcurrency)
	results := make(chan uploadResult, s.numParts)

	var wg sync.WaitGroup
	for i := 0; i < len(encryptedChunks); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			etag, err := s.uploadChunkWithRetry(idx, encryptedChunks[idx])

			results <- uploadResult{index: idx, etag: etag, err: err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	parts := make([]CompletedPart, s.numParts)
	var firstError error

	resultsCollected := 0
	for result := range results {
		resultsCollected++
		if result.err != nil && firstError == nil {
			firstError = result.err
		}
		if result.err == nil {
			parts[result.index] = CompletedPart{
				PartNumber: result.index + 1,
				ETag:       result.etag,
			}
		}
	}

	fmt.Printf("DEBUG uploadConcurrently: Results collection complete. FirstError=%v\n", firstError)

	if firstError != nil {
		return nil, "", firstError
	}

	overallHasher := sha1.New()
	for _, chunk := range encryptedChunks {
		overallHasher.Write(chunk)
	}
	overallHash := hex.EncodeToString(overallHasher.Sum(nil))

	return parts, overallHash, nil
}

// uploadChunkWithRetry uploads a single chunk with exponential backoff retry
func (s *multipartUploadState) uploadChunkWithRetry(partIndex int, encryptedData []byte) (string, error) {
	const maxRetries = 3
	const baseDelay = 1 * time.Second

	uploadURL := s.startResp.Uploads[0].URLs[partIndex]

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			time.Sleep(delay)
		}

		result, err := Transfer(s.cfg, uploadURL, bytes.NewReader(encryptedData), int64(len(encryptedData)))
		if err == nil {
			return result.ETag, nil
		}

		lastErr = err
		if !isRetryableError(err) {
			break
		}
	}

	return "", fmt.Errorf("chunk %d upload failed after %d retries: %w", partIndex+1, maxRetries, lastErr)
}

// isRetryableError determines if an error should be retried
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	if contains(errStr, "400") || contains(errStr, "401") || contains(errStr, "403") || contains(errStr, "404") {
		return false
	}

	return true
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
