package buckets

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/internxt/rclone-adapter/config"
)

func UploadFile(ctx context.Context, cfg *config.Config, filePath, targetFolderUUID string, modTime time.Time) (*CreateMetaResponse, error) {
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	plainSize := int64(len(raw))
	var ph [32]byte
	if _, err := rand.Read(ph[:]); err != nil {
		return nil, fmt.Errorf("cannot generate random index: %w", err)
	}

	plainIndex := hex.EncodeToString(ph[:])
	fileKey, iv, err := GenerateFileKey(cfg.Mnemonic, cfg.Bucket, plainIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to generate file key: %w", err)
	}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer f.Close()
	encReader, err := EncryptReader(f, fileKey, iv)
	if err != nil {
		return nil, fmt.Errorf("failed to create encrypt reader: %w", err)
	}
	// Compute hash: RIPEMD-160(SHA-256(encrypted_data)) - matches web client
	sha256Hasher := sha256.New()
	r := io.TeeReader(encReader, sha256Hasher)
	specs := []UploadPartSpec{{Index: 0, Size: plainSize}}
	startResp, err := StartUpload(ctx, cfg, cfg.Bucket, specs)
	if err != nil {
		return nil, fmt.Errorf("failed to start upload: %w", err)
	}
	part := startResp.Uploads[0]
	uploadURL := part.URL
	if len(part.URLs) > 0 {
		uploadURL = part.URLs[0]
	}
	if _, err := Transfer(ctx, cfg, uploadURL, r, plainSize); err != nil {
		return nil, fmt.Errorf("failed to transfer file data: %w", err)
	}
	encIndex := hex.EncodeToString(ph[:])
	// Compute RIPEMD-160(SHA-256) to match web client
	sha256Result := sha256Hasher.Sum(nil)
	partHash := ComputeFileHash(sha256Result)

	finishResp, err := FinishUpload(ctx, cfg, cfg.Bucket, encIndex, []Shard{{Hash: partHash, UUID: part.UUID}})
	if err != nil {
		return nil, fmt.Errorf("failed to finish upload: %w", err)
	}
	base := filepath.Base(filePath)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	ext := strings.TrimPrefix(filepath.Ext(base), ".")
	meta, err := CreateMetaFile(ctx, cfg, name, cfg.Bucket, finishResp.ID, "03-aes", targetFolderUUID, name, ext, plainSize, modTime)
	if err != nil {
		return nil, fmt.Errorf("failed to create file metadata: %w", err)
	}
	return meta, nil
}

// UploadFileStream uploads data from the provided io.Reader into Internxt,
// encrypting it on the fly and creating the metadata file in the target folder.
// It returns the CreateMetaResponse of the created file entry.
func UploadFileStream(ctx context.Context, cfg *config.Config, targetFolderUUID, fileName string, in io.Reader, plainSize int64, modTime time.Time) (*CreateMetaResponse, error) {
	var ph [32]byte
	if _, err := rand.Read(ph[:]); err != nil {
		return nil, fmt.Errorf("cannot generate random index: %w", err)
	}
	plainIndex := hex.EncodeToString(ph[:])

	fileKey, iv, err := GenerateFileKey(cfg.Mnemonic, cfg.Bucket, plainIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to generate file key: %w", err)
	}

	// Start the API call asynchronously to overlap with encryption setup
	type startResult struct {
		resp *StartUploadResp
		err  error
	}
	startChan := make(chan startResult, 1)
	specs := []UploadPartSpec{{Index: 0, Size: plainSize}}

	go func() {
		resp, err := StartUpload(ctx, cfg, cfg.Bucket, specs)
		startChan <- startResult{resp: resp, err: err}
	}()

	// While StartUpload is in flight, set up encryption and pre-read data
	encReader, err := EncryptReader(in, fileKey, iv)
	if err != nil {
		return nil, fmt.Errorf("failed to create encrypt reader: %w", err)
	}

	// Compute hash: RIPEMD-160(SHA-256(encrypted_data)) - matches web client
	sha256Hasher := sha256.New()
	r := io.TeeReader(encReader, sha256Hasher)

	// Pre-read a buffer to reduce transfer startup latency
	// Use 5MB or file size, whichever is smaller
	bufSize := min(plainSize, int64(5 * 1024 * 1024))

	preBuf := make([]byte, bufSize)
	preReadN, preReadErr := io.ReadFull(r, preBuf)
	if preReadErr != nil && preReadErr != io.ErrUnexpectedEOF && preReadErr != io.EOF {
		return nil, fmt.Errorf("failed to pre-read data: %w", preReadErr)
	}
	preBuf = preBuf[:preReadN]

	// Wait for StartUpload to complete
	startRes := <-startChan
	if startRes.err != nil {
		return nil, fmt.Errorf("failed to start upload: %w", startRes.err)
	}
	startResp := startRes.resp

	if len(startResp.Uploads) == 0 {
		return nil, fmt.Errorf("startResp.Uploads is empty")
	}

	part := startResp.Uploads[0]
	uploadURL := part.URL
	if len(part.URLs) > 0 {
		uploadURL = part.URLs[0]
	}

	// Transfer using pre-buffered data + remaining stream
	multiReader := io.MultiReader(bytes.NewReader(preBuf), r)
	if _, err := Transfer(ctx, cfg, uploadURL, multiReader, plainSize); err != nil {
		return nil, fmt.Errorf("failed to transfer file data: %w", err)
	}

	encIndex := hex.EncodeToString(ph[:])
	// Compute RIPEMD-160(SHA-256) to match web client
	sha256Result := sha256Hasher.Sum(nil)
	partHash := ComputeFileHash(sha256Result)
	finishResp, err := FinishUpload(ctx, cfg, cfg.Bucket, encIndex, []Shard{{Hash: partHash, UUID: part.UUID}})
	if err != nil {
		return nil, fmt.Errorf("failed to finish upload: %w", err)
	}

	base := filepath.Base(fileName)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	ext := strings.TrimPrefix(filepath.Ext(base), ".")
	meta, err := CreateMetaFile(ctx, cfg, name, cfg.Bucket, finishResp.ID, "03-aes", targetFolderUUID, name, ext, plainSize, modTime)
	if err != nil {
		return nil, fmt.Errorf("failed to create file metadata: %w", err)
	}
	return meta, nil
}

// UploadFileStreamMultipart uploads data from an io.Reader using multipart upload.
// This is intended for large files (>100MB) and splits the file into multiple chunks
func UploadFileStreamMultipart(ctx context.Context, cfg *config.Config, targetFolderUUID, fileName string, in io.Reader, plainSize int64, modTime time.Time) (*CreateMetaResponse, error) {
	state, err := newMultipartUploadState(cfg, plainSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize multipart upload state: %w", err)
	}

	shard, err := state.executeMultipartUpload(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("failed to execute multipart upload: %w", err)
	}

	finishResp, err := FinishMultipartUpload(ctx, cfg, cfg.Bucket, state.encIndex, *shard)
	if err != nil {
		return nil, fmt.Errorf("failed to finish multipart upload: %w", err)
	}

	base := filepath.Base(fileName)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	ext := strings.TrimPrefix(filepath.Ext(base), ".")
	meta, err := CreateMetaFile(ctx, cfg, name, cfg.Bucket, finishResp.ID, "03-aes", targetFolderUUID, name, ext, plainSize, modTime)
	if err != nil {
		return nil, fmt.Errorf("failed to create file metadata: %w", err)
	}

	return meta, nil
}

// UploadFileStreamAuto automatically chooses between single-part and multipart upload
func UploadFileStreamAuto(ctx context.Context, cfg *config.Config, targetFolderUUID, fileName string, in io.Reader, plainSize int64, modTime time.Time) (*CreateMetaResponse, error) {
	if plainSize >= config.DefaultMultipartMinSize {
		return UploadFileStreamMultipart(ctx, cfg, targetFolderUUID, fileName, in, plainSize, modTime)
	}
	return UploadFileStream(ctx, cfg, targetFolderUUID, fileName, in, plainSize, modTime)
}
