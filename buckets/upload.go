package buckets

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
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
		return nil, err
	}
	plainSize := int64(len(raw))
	var ph [32]byte
	if _, err := rand.Read(ph[:]); err != nil {
		return nil, fmt.Errorf("cannot generate random index: %w", err)
	}

	plainIndex := hex.EncodeToString(ph[:])
	fileKey, iv, err := GenerateFileKey(cfg.Mnemonic, cfg.Bucket, plainIndex)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	encReader, err := EncryptReader(f, fileKey, iv)
	if err != nil {
		return nil, err
	}
	sha256Hasher := sha256.New()
	sha1Hasher := sha1.New()
	r := io.TeeReader(encReader, sha256Hasher)
	r = io.TeeReader(r, sha1Hasher)
	specs := []UploadPartSpec{{Index: 0, Size: plainSize}}
	startResp, err := StartUpload(ctx, cfg, cfg.Bucket, specs)
	if err != nil {
		return nil, err
	}
	part := startResp.Uploads[0]
	uploadURL := part.URL
	if len(part.URLs) > 0 {
		uploadURL = part.URLs[0]
	}
	if _, err := Transfer(ctx, cfg, uploadURL, r, plainSize); err != nil {
		return nil, err
	}
	encIndex := hex.EncodeToString(ph[:])
	partHash := hex.EncodeToString(sha1Hasher.Sum(nil))

	finishResp, err := FinishUpload(ctx, cfg, cfg.Bucket, encIndex, []Shard{{Hash: partHash, UUID: part.UUID}})
	if err != nil {
		return nil, err
	}
	base := filepath.Base(filePath)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	ext := strings.TrimPrefix(filepath.Ext(base), ".")
	meta, err := CreateMetaFile(ctx, cfg, name, cfg.Bucket, finishResp.ID, "03-aes", targetFolderUUID, name, ext, plainSize, modTime)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	encReader, err := EncryptReader(in, fileKey, iv)
	if err != nil {
		return nil, err
	}

	sha256Hasher := sha256.New()
	sha1Hasher := sha1.New()
	r := io.TeeReader(encReader, sha256Hasher)
	r = io.TeeReader(r, sha1Hasher)

	specs := []UploadPartSpec{{Index: 0, Size: plainSize}}
	startResp, err := StartUpload(ctx, cfg, cfg.Bucket, specs)
	if err != nil {
		return nil, err
	}

	if len(startResp.Uploads) == 0 {
		return nil, fmt.Errorf("startResp.Uploads is empty")
	}

	part := startResp.Uploads[0]
	uploadURL := part.URL
	if len(part.URLs) > 0 {
		uploadURL = part.URLs[0]
	}

	if _, err := Transfer(ctx, cfg, uploadURL, r, plainSize); err != nil {
		return nil, err
	}

	encIndex := hex.EncodeToString(ph[:])
	partHash := hex.EncodeToString(sha1Hasher.Sum(nil))
	finishResp, err := FinishUpload(ctx, cfg, cfg.Bucket, encIndex, []Shard{{Hash: partHash, UUID: part.UUID}})
	if err != nil {
		return nil, err
	}

	base := filepath.Base(fileName)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	ext := strings.TrimPrefix(filepath.Ext(base), ".")
	meta, err := CreateMetaFile(ctx, cfg, name, cfg.Bucket, finishResp.ID, "03-aes", targetFolderUUID, name, ext, plainSize, modTime)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// UploadFileStreamMultipart uploads data from an io.Reader using multipart upload.
// This is intended for large files (>100MB) and splits the file into multiple chunks
func UploadFileStreamMultipart(ctx context.Context, cfg *config.Config, targetFolderUUID, fileName string, in io.Reader, plainSize int64, modTime time.Time) (*CreateMetaResponse, error) {
	state, err := newMultipartUploadState(cfg, plainSize)
	if err != nil {
		return nil, err
	}

	shard, err := state.executeMultipartUpload(ctx, in)
	if err != nil {
		return nil, err
	}

	finishResp, err := FinishMultipartUpload(ctx, cfg, cfg.Bucket, state.encIndex, *shard)
	if err != nil {
		return nil, err
	}

	base := filepath.Base(fileName)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	ext := strings.TrimPrefix(filepath.Ext(base), ".")
	meta, err := CreateMetaFile(ctx, cfg, name, cfg.Bucket, finishResp.ID, "03-aes", targetFolderUUID, name, ext, plainSize, modTime)
	if err != nil {
		return nil, err
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
