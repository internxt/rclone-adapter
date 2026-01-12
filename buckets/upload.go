package buckets

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/errors"
	"github.com/internxt/rclone-adapter/thumbnails"
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

	encReader, err := EncryptReader(in, fileKey, iv)
	if err != nil {
		return nil, fmt.Errorf("failed to create encrypt reader: %w", err)
	}

	// Compute hash: RIPEMD-160(SHA-256(encrypted_data)) - matches web client
	sha256Hasher := sha256.New()
	r := io.TeeReader(encReader, sha256Hasher)

	// Handle unknown size by buffering entire stream
	var preBuf []byte
	if plainSize < 0 {
		fmt.Printf("[DEBUG] UploadFileStream: Unknown size, buffering entire stream...\n")
		preBuf, err = io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read stream (unknown size): %w", err)
		}
		plainSize = int64(len(preBuf))
	} else {
		// Pre-read a buffer to reduce transfer startup latency
		// Use 5MB or file size, whichever is smaller
		bufSize := min(plainSize, int64(5 * 1024 * 1024))
		preBuf = make([]byte, bufSize)
		preReadN, preReadErr := io.ReadFull(r, preBuf)
		if preReadErr != nil && preReadErr != io.ErrUnexpectedEOF && preReadErr != io.EOF {
			return nil, fmt.Errorf("failed to pre-read data: %w", preReadErr)
		}
		preBuf = preBuf[:preReadN]
	}

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
	const maxUnknownSizeBuffer = 1024 * 1024 * 1024 // 1GB limit
	var bufferedData []byte
	if plainSize < 0 {

		// Use LimitReader to prevent OOM on huge streams
		limitedReader := io.LimitReader(in, maxUnknownSizeBuffer+1)
		var err error
		bufferedData, err = io.ReadAll(limitedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to buffer unknown-size stream: %w", err)
		}

		if int64(len(bufferedData)) > maxUnknownSizeBuffer {
			return nil, fmt.Errorf("unknown-size upload exceeds %d byte limit - size must be known for files larger than 1GB", maxUnknownSizeBuffer)
		}

		plainSize = int64(len(bufferedData))
		in = bytes.NewReader(bufferedData)
	}

	var capturedData *bytes.Buffer
	var capturedReader io.Reader = in

	ext := strings.TrimPrefix(filepath.Ext(fileName), ".")
	if thumbnails.IsSupportedFormat(ext) && plainSize > 0 && plainSize <= config.MaxThumbnailSourceSize {
		capturedData = &bytes.Buffer{}
		capturedReader = io.TeeReader(in, capturedData)
	}

	var meta *CreateMetaResponse
	var err error
	if plainSize >= config.DefaultMultipartMinSize {
		meta, err = UploadFileStreamMultipart(ctx, cfg, targetFolderUUID, fileName, capturedReader, plainSize, modTime)
	} else {
		meta, err = UploadFileStream(ctx, cfg, targetFolderUUID, fileName, capturedReader, plainSize, modTime)
	}

	if err != nil {
		return nil, err
	}

	if capturedData != nil && capturedData.Len() > 0 {
		go uploadThumbnailAsync(ctx, cfg, meta.UUID, ext, capturedData.Bytes())
	}

	return meta, nil
}

// uploadThumbnailAsync handles thumbnail upload in a background goroutine
func uploadThumbnailAsync(ctx context.Context, cfg *config.Config, fileUUID, fileType string, originalData []byte) {
	bgCtx := context.Background()

	if err := uploadThumbnail(bgCtx, cfg, fileUUID, fileType, originalData); err != nil {
		fmt.Printf("[WARN] Thumbnail upload failed for %s: %v\n", fileUUID, err)
	}
}

// uploadThumbnail generates and uploads a thumbnail for the given file
func uploadThumbnail(ctx context.Context, cfg *config.Config, fileUUID, fileType string, originalData []byte) error {
	thumbReader, thumbSize, thumbCfg, err := thumbnails.GenerateAndPrepare(fileType, originalData)
	if err != nil {
		return fmt.Errorf("failed to generate thumbnail: %w", err)
	}

	thumbFileName := fmt.Sprintf("thumb_%s.png", fileUUID)
	meta, err := UploadFileStream(ctx, cfg, cfg.RootFolderID, thumbFileName, thumbReader, thumbSize, time.Now())
	if err != nil {
		return fmt.Errorf("failed to upload thumbnail file: %w", err)
	}

	req := thumbnails.CreateThumbnailMetadata(
		fileUUID,
		meta.Bucket,
		meta.FileID,
		meta.EncryptVersion,
		thumbSize,
		thumbCfg,
	)

	if err := createThumbnailAPI(ctx, cfg, req); err != nil {
		return fmt.Errorf("failed to register thumbnail: %w", err)
	}

	return nil
}

// createThumbnailAPI registers a thumbnail via POST /drive/files/thumbnail
func createThumbnailAPI(ctx context.Context, cfg *config.Config, req thumbnails.CreateThumbnailRequest) error {
	endpoint := cfg.Endpoints.Drive().Files().Thumbnail()

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal thumbnail request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create thumbnail request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+cfg.Token)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to execute thumbnail request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return errors.NewHTTPError(resp, "create thumbnail")
	}

	return nil
}
