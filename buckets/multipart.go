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
}

// uploadResult holds the result of a single chunk upload
type uploadResult struct {
	index int
	shard Shard
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
func (s *multipartUploadState) executeMultipartUpload(reader io.Reader) ([]Shard, error) {
	fmt.Printf("DEBUG executeMultipartUpload: Starting multipart upload for %d parts\n", s.numParts)
	specs := make([]UploadPartSpec, s.numParts)
	for i := int64(0); i < s.numParts; i++ {
		partSize := s.chunkSize
		if i == s.numParts-1 {
			partSize = s.totalSize - (i * s.chunkSize)
		}
		specs[i] = UploadPartSpec{
			Index: int(i),
			Size:  partSize,
		}
	}

	fmt.Printf("DEBUG executeMultipartUpload: Calling StartUpload...\n")
	var err error
	s.startResp, err = StartUpload(s.cfg, s.cfg.Bucket, specs)
	if err != nil {
		return nil, fmt.Errorf("failed to start multipart upload: %w", err)
	}

	if len(s.startResp.Uploads) != int(s.numParts) {
		return nil, fmt.Errorf("expected %d upload parts, got %d", s.numParts, len(s.startResp.Uploads))
	}
	fmt.Printf("DEBUG executeMultipartUpload: StartUpload completed, got %d upload URLs\n", len(s.startResp.Uploads))

	fmt.Printf("DEBUG executeMultipartUpload: Encrypting %d chunks...\n", s.numParts)
	encryptedChunks, err := s.encryptAllChunks(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt chunks: %w", err)
	}
	fmt.Printf("DEBUG executeMultipartUpload: Encryption completed\n")

	fmt.Printf("DEBUG executeMultipartUpload: Uploading chunks concurrently (max %d concurrent)...\n", s.maxConcurrency)
	shards, err := s.uploadConcurrently(encryptedChunks)
	if err != nil {
		return nil, fmt.Errorf("failed to upload chunks: %w", err)
	}
	fmt.Printf("DEBUG executeMultipartUpload: All chunks uploaded successfully\n")

	return shards, nil
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
func (s *multipartUploadState) uploadConcurrently(encryptedChunks [][]byte) ([]Shard, error) {
	semaphore := make(chan struct{}, s.maxConcurrency)
	results := make(chan uploadResult, s.numParts)

	fmt.Printf("DEBUG uploadConcurrently: Launching %d upload goroutines\n", len(encryptedChunks))

	var wg sync.WaitGroup
	for i := 0; i < len(encryptedChunks); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			fmt.Printf("DEBUG uploadConcurrently: Starting upload of chunk %d/%d (size=%d bytes)\n", idx, len(encryptedChunks), len(encryptedChunks[idx]))

			shard, err := s.uploadChunkWithRetry(idx, encryptedChunks[idx])
			if err != nil {
				fmt.Printf("DEBUG uploadConcurrently: Chunk %d FAILED: %v\n", idx, err)
			} else {
				fmt.Printf("DEBUG uploadConcurrently: Chunk %d completed successfully\n", idx)
			}
			results <- uploadResult{index: idx, shard: shard, err: err}
		}(i)
	}

	go func() {
		wg.Wait()
		fmt.Printf("DEBUG uploadConcurrently: All upload goroutines completed, closing results channel\n")
		close(results)
	}()

	shards := make([]Shard, s.numParts)
	var firstError error

	fmt.Printf("DEBUG uploadConcurrently: Collecting results...\n")
	resultsCollected := 0
	for result := range results {
		resultsCollected++
		fmt.Printf("DEBUG uploadConcurrently: Received result %d/%d (index=%d, hasError=%v)\n", resultsCollected, s.numParts, result.index, result.err != nil)
		if result.err != nil && firstError == nil {
			firstError = result.err
		}
		if result.err == nil {
			shards[result.index] = result.shard
		}
	}

	fmt.Printf("DEBUG uploadConcurrently: Results collection complete. FirstError=%v\n", firstError)

	if firstError != nil {
		return nil, firstError
	}

	return shards, nil
}

// uploadChunkWithRetry uploads a single chunk with exponential backoff retry
func (s *multipartUploadState) uploadChunkWithRetry(partIndex int, encryptedData []byte) (Shard, error) {
	const maxRetries = 3
	const baseDelay = 1 * time.Second

	part := s.startResp.Uploads[partIndex]

	sha1Hash := sha1.New()
	sha1Hash.Write(encryptedData)
	partHash := hex.EncodeToString(sha1Hash.Sum(nil))

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			time.Sleep(delay)
		}

		err := Transfer(s.cfg, part, bytes.NewReader(encryptedData), int64(len(encryptedData)))
		if err == nil {
			return Shard{
				Hash: partHash,
				UUID: part.UUID,
			}, nil
		}

		lastErr = err

		if !isRetryableError(err) {
			break
		}
	}

	return Shard{}, fmt.Errorf("chunk %d upload failed after %d retries: %w", partIndex, maxRetries, lastErr)
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
