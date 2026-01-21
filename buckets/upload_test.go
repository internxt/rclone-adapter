package buckets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/config"
)

// mockMultiEndpointServer is an alias for MockMultiEndpointServer
type mockMultiEndpointServer = MockMultiEndpointServer

func newMockMultiEndpointServer() *mockMultiEndpointServer {
	return NewMockMultiEndpointServer()
}

// TestUploadFileStream tests uploading from an io.Reader
func TestUploadFileStream(t *testing.T) {
	testContent := []byte("Streaming upload test content")

	testCases := []struct {
		name          string
		content       []byte
		fileName      string
		setupMock     func(*mockMultiEndpointServer)
		expectError   bool
		errorContains string
	}{
		{
			name:     "successful stream upload",
			content:  testContent,
			fileName: "stream-file.dat",
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{
							{UUID: "stream-uuid", URL: m.URL() + "/upload/stream"},
						},
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"stream-etag\"")
					w.WriteHeader(http.StatusOK)
				}
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := FinishUploadResp{ID: "stream-file-id"}
					json.NewEncoder(w).Encode(resp)
				}
			},
			expectError: false,
		},
		{
			name:     "error - empty Uploads array",
			content:  testContent,
			fileName: "test.dat",
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{}, // Empty array
					}
					json.NewEncoder(w).Encode(resp)
				}
			},
			expectError:   true,
			errorContains: "startResp.Uploads is empty",
		},
		{
			name:     "error - Transfer fails",
			content:  testContent,
			fileName: "test.dat",
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{{UUID: "uuid", URL: m.URL() + "/upload/fail"}},
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			},
			expectError:   true,
			errorContains: "failed to transfer file data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := newMockMultiEndpointServer()
			defer mockServer.Close()

			tc.setupMock(mockServer)

			cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
				c.Bucket = TestBucket2
			})

			reader := bytes.NewReader(tc.content)
			result, err := UploadFileStream(context.Background(), cfg, TestFolderUUID, tc.fileName, reader, int64(len(tc.content)), time.Now())

			if tc.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain '%s', got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result == nil {
					t.Error("expected result, got nil")
				}
			}
		})
	}
}

// TestUploadFileStreamMultipart tests multipart upload functionality
func TestUploadFileStreamMultipart(t *testing.T) {
	// Create content larger than chunk size to trigger multipart
	largeContent := make([]byte, config.DefaultChunkSize*2+1000)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	testCases := []struct {
		name          string
		content       []byte
		fileName      string
		setupMock     func(*mockMultiEndpointServer)
		expectError   bool
		errorContains string
	}{
		{
			name:     "successful multipart upload",
			content:  largeContent,
			fileName: "large-file.bin",
			setupMock: func(m *mockMultiEndpointServer) {
				m.multipartStartHandler = func(w http.ResponseWriter, r *http.Request) {
					// Get number of parts from query parameter
					numParts := 3
					if mp := r.URL.Query().Get("multiparts"); mp != "" {
						fmt.Sscanf(mp, "%d", &numParts)
					}

					// Generate URLs for each part
					urls := make([]string, numParts)
					for i := range urls {
						urls[i] = m.URL() + "/upload/multipart"
					}

					resp := StartUploadResp{
						Uploads: []UploadPart{{
							UUID:     "multipart-uuid",
							UploadId: "multipart-upload-id",
							URLs:     urls,
						}},
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"part-etag\"")
					w.WriteHeader(http.StatusOK)
				}
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := FinishUploadResp{ID: "multipart-file-id"}
					json.NewEncoder(w).Encode(resp)
				}
			},
			expectError: false,
		},
		{
			name:     "error - multipart start fails",
			content:  largeContent,
			fileName: "large-fail.bin",
			setupMock: func(m *mockMultiEndpointServer) {
				m.multipartStartHandler = func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("multipart start error"))
				}
			},
			expectError:   true,
			errorContains: "failed to execute multipart upload",
		},
		{
			name:     "error - finish multipart fails",
			content:  largeContent,
			fileName: "large-finish-fail.bin",
			setupMock: func(m *mockMultiEndpointServer) {
				m.multipartStartHandler = func(w http.ResponseWriter, r *http.Request) {
					numParts := 3
					if mp := r.URL.Query().Get("multiparts"); mp != "" {
						fmt.Sscanf(mp, "%d", &numParts)
					}
					urls := make([]string, numParts)
					for i := range urls {
						urls[i] = m.URL() + "/upload/multipart"
					}
					uploadParts := []UploadPart{{
						UUID:     "uuid",
						UploadId: "upload-id",
						URLs:     urls,
					}}
					resp := StartUploadResp{
						Uploads: uploadParts,
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"etag\"")
					w.WriteHeader(http.StatusOK)
				}
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte("finish failed"))
				}
			},
			expectError:   true,
			errorContains: "failed to finish multipart upload",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := newMockMultiEndpointServer()
			defer mockServer.Close()

			tc.setupMock(mockServer)

			cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
				c.Bucket = TestBucket3
			})

			reader := bytes.NewReader(tc.content)
			result, err := UploadFileStreamMultipart(context.Background(), cfg, TestFolderUUID, tc.fileName, reader, int64(len(tc.content)), time.Now())

			if tc.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain '%s', got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result == nil {
					t.Error("expected result, got nil")
				}
			}
		})
	}
}

// TestUploadFileStreamAuto tests automatic routing between single-part and multipart uploads
func TestUploadFileStreamAuto(t *testing.T) {
	testCases := []struct {
		name              string
		fileSize          int64
		expectedMultipart bool
		setupMock         func(*mockMultiEndpointServer)
	}{
		{
			name:              "small file - uses single-part",
			fileSize:          1024 * 1024, // 1 MB
			expectedMultipart: false,
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{{UUID: "uuid", URL: m.URL() + "/upload/single"}},
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"etag\"")
					w.WriteHeader(http.StatusOK)
				}
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					json.NewEncoder(w).Encode(FinishUploadResp{ID: "file-id"})
				}
				m.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
					json.NewEncoder(w).Encode(CreateMetaResponse{UUID: "uuid", FileID: "file-id"})
				}
			},
		},
		{
			name:              "large file - uses multipart",
			fileSize:          config.DefaultMultipartMinSize + 1000,
			expectedMultipart: true,
			setupMock: func(m *mockMultiEndpointServer) {
				m.multipartStartHandler = func(w http.ResponseWriter, r *http.Request) {
					numParts := 3
					if mp := r.URL.Query().Get("multiparts"); mp != "" {
						fmt.Sscanf(mp, "%d", &numParts)
					}
					urls := make([]string, numParts)
					for i := range urls {
						urls[i] = m.URL() + "/upload/multi"
					}
					json.NewEncoder(w).Encode(StartUploadResp{
						Uploads: []UploadPart{{
							UUID:     "uuid",
							UploadId: "upload-id",
							URLs:     urls,
						}},
					})
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"etag\"")
					w.WriteHeader(http.StatusOK)
				}
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					json.NewEncoder(w).Encode(FinishUploadResp{ID: "file-id"})
				}
				m.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
					json.NewEncoder(w).Encode(CreateMetaResponse{UUID: "uuid", FileID: "file-id"})
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := newMockMultiEndpointServer()
			defer mockServer.Close()

			tc.setupMock(mockServer)

			cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
				c.Bucket = TestBucket4
			})

			content := make([]byte, tc.fileSize)
			reader := bytes.NewReader(content)

			result, err := UploadFileStreamAuto(context.Background(), cfg, TestFolderUUID, "auto-file.dat", reader, tc.fileSize, time.Now())

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result == nil {
				t.Error("expected result, got nil")
			}
		})
	}
}

// TestUploadFileStreamContextCancellation tests context cancellation handling
func TestUploadFileStreamContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	cfg := newTestConfigWithSetup("http://localhost", func(c *config.Config) {
		c.Bucket = TestBucket6
	})

	content := []byte("test content")
	reader := bytes.NewReader(content)

	_, err := UploadFileStream(ctx, cfg, TestFolderUUID, "test.txt", reader, int64(len(content)), time.Now())
	if err == nil {
		t.Error("expected error due to cancelled context, got nil")
	}
}
<<<<<<< HEAD
=======

// TestUploadFileNameParsing tests file name and extension parsing
func TestUploadFileNameParsing(t *testing.T) {
	tmpDir := t.TempDir()

	testCases := []struct {
		fileName     string
		expectedName string
		expectedExt  string
	}{
		{"simple.txt", "simple", "txt"},
		{"multiple.dots.tar.gz", "multiple.dots.tar", "gz"},
		{"noextension", "noextension", ""},
		{".hidden", "", "hidden"},
	}

	for _, tc := range testCases {
		t.Run(tc.fileName, func(t *testing.T) {
			testFilePath := filepath.Join(tmpDir, tc.fileName)
			os.WriteFile(testFilePath, []byte("content"), 0644)

			mockServer := newMockMultiEndpointServer()
			defer mockServer.Close()

			var capturedName, capturedType string
			mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(StartUploadResp{
					Uploads: []UploadPart{{UUID: "uuid", URL: mockServer.URL() + "/upload"}},
				})
			}
			mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", "\"etag\"")
				w.WriteHeader(http.StatusOK)
			}
			mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(FinishUploadResp{ID: TestFileID})
			}
			mockServer.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
				var req CreateMetaRequest
				json.NewDecoder(r.Body).Decode(&req)
				capturedName = req.PlainName
				capturedType = req.Type
				json.NewEncoder(w).Encode(CreateMetaResponse{UUID: "uuid", FileID: TestFileID})
			}

			cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
				c.Bucket = TestBucket7
			})

			_, err := UploadFile(context.Background(), cfg, testFilePath, TestFolderUUID, time.Now())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedName != tc.expectedName {
				t.Errorf("expected name '%s', got '%s'", tc.expectedName, capturedName)
			}
			if capturedType != tc.expectedExt {
				t.Errorf("expected type '%s', got '%s'", tc.expectedExt, capturedType)
			}
		})
	}
}

// TestEncryptionSetup tests the encryptionSetup helper function
func TestEncryptionSetup(t *testing.T) {
	testContent := []byte("test content for encryption")

	t.Run("successful encryption setup", func(t *testing.T) {
		cfg := &config.Config{
			Mnemonic: TestMnemonic,
			Bucket:   TestBucket7,
		}

		reader := bytes.NewReader(testContent)
		encryptedReader, hasher, encIndex, err := encryptionSetup(reader, cfg)

		if err != nil {
			t.Fatalf("encryptionSetup() error = %v, want nil", err)
		}

		if encryptedReader == nil {
			t.Error("encryptedReader should not be nil")
		}

		if hasher == nil {
			t.Error("hasher should not be nil")
		}

		if encIndex == "" {
			t.Error("encIndex should not be empty")
		}

		if len(encIndex) != 64 {
			t.Errorf("encIndex length = %d, want 64", len(encIndex))
		}
	})

	t.Run("error - invalid mnemonic", func(t *testing.T) {
		cfg := &config.Config{
			Mnemonic: "invalid mnemonic that is not a valid BIP39 phrase",
			Bucket:   TestBucket7,
		}

		reader := bytes.NewReader(testContent)
		_, _, _, err := encryptionSetup(reader, cfg)

		if err == nil {
			t.Error("encryptionSetup() with invalid mnemonic should return error")
		}

		if !strings.Contains(err.Error(), "failed to generate file key") {
			t.Errorf("error should contain 'failed to generate file key', got: %v", err)
		}
	})

	t.Run("error - invalid bucket ID", func(t *testing.T) {
		cfg := &config.Config{
			Mnemonic: TestMnemonic,
			Bucket:   "invalid-bucket-id-not-hex",
		}

		reader := bytes.NewReader(testContent)
		_, _, _, err := encryptionSetup(reader, cfg)

		if err == nil {
			t.Error("encryptionSetup() with invalid bucket should return error")
		}
	})
}

// TestUploadEncryptedData tests the uploadEncryptedData helper function
func TestUploadEncryptedData(t *testing.T) {
	testContent := []byte("encrypted test content")

	t.Run("successful upload", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		var transferredData []byte
		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{
						UUID: "test-uuid",
						URL:  mockServer.URL() + "/upload",
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			data, _ := io.ReadAll(r.Body)
			transferredData = data
			w.Header().Set("ETag", "\"test-etag\"")
			w.WriteHeader(http.StatusOK)
		}

		mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := FinishUploadResp{
				ID: "network-file-id-123",
			}
			json.NewEncoder(w).Encode(resp)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
			c.Bucket = TestBucket7
		})

		reader := bytes.NewReader(testContent)
		encryptedReader, hasher, encIndex, err := encryptionSetup(reader, cfg)
		if err != nil {
			t.Fatalf("encryptionSetup failed: %v", err)
		}

		fileID, err := uploadEncryptedData(context.Background(), cfg, encryptedReader, hasher, encIndex, int64(len(testContent)))

		if err != nil {
			t.Fatalf("uploadEncryptedData() error = %v, want nil", err)
		}

		if fileID != "network-file-id-123" {
			t.Errorf("fileID = %s, want 'network-file-id-123'", fileID)
		}

		if len(transferredData) == 0 {
			t.Error("no data was transferred")
		}
	})

	t.Run("error - StartUpload fails", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("start upload error"))
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
			c.Bucket = TestBucket7
		})

		reader := bytes.NewReader(testContent)
		encryptedReader, hasher, encIndex, err := encryptionSetup(reader, cfg)
		if err != nil {
			t.Fatalf("encryptionSetup failed: %v", err)
		}

		_, err = uploadEncryptedData(context.Background(), cfg, encryptedReader, hasher, encIndex, int64(len(testContent)))

		if err == nil {
			t.Error("uploadEncryptedData() should return error when StartUpload fails")
		}

		if !strings.Contains(err.Error(), "failed to start upload") {
			t.Errorf("error should contain 'failed to start upload', got: %v", err)
		}
	})

	t.Run("error - empty uploads array", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := StartUploadResp{
				Uploads: []UploadPart{},
			}
			json.NewEncoder(w).Encode(resp)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
			c.Bucket = TestBucket7
		})

		reader := bytes.NewReader(testContent)
		encryptedReader, hasher, encIndex, err := encryptionSetup(reader, cfg)
		if err != nil {
			t.Fatalf("encryptionSetup failed: %v", err)
		}

		_, err = uploadEncryptedData(context.Background(), cfg, encryptedReader, hasher, encIndex, int64(len(testContent)))

		if err == nil {
			t.Error("uploadEncryptedData() should return error when Uploads array is empty")
		}

		if !strings.Contains(err.Error(), "startResp.Uploads is empty") {
			t.Errorf("error should contain 'startResp.Uploads is empty', got: %v", err)
		}
	})

	t.Run("error - Transfer fails", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{
						UUID: "test-uuid",
						URL:  mockServer.URL() + "/upload",
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("transfer error"))
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
			c.Bucket = TestBucket7
		})

		reader := bytes.NewReader(testContent)
		encryptedReader, hasher, encIndex, err := encryptionSetup(reader, cfg)
		if err != nil {
			t.Fatalf("encryptionSetup failed: %v", err)
		}

		_, err = uploadEncryptedData(context.Background(), cfg, encryptedReader, hasher, encIndex, int64(len(testContent)))

		if err == nil {
			t.Error("uploadEncryptedData() should return error when Transfer fails")
		}

		if !strings.Contains(err.Error(), "failed to transfer data") {
			t.Errorf("error should contain 'failed to transfer data', got: %v", err)
		}
	})

	t.Run("error - FinishUpload fails", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{
						UUID: "test-uuid",
						URL:  mockServer.URL() + "/upload",
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", "\"test-etag\"")
			w.WriteHeader(http.StatusOK)
		}

		mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("finish upload error"))
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
			c.Bucket = TestBucket7
		})

		reader := bytes.NewReader(testContent)
		encryptedReader, hasher, encIndex, err := encryptionSetup(reader, cfg)
		if err != nil {
			t.Fatalf("encryptionSetup failed: %v", err)
		}

		_, err = uploadEncryptedData(context.Background(), cfg, encryptedReader, hasher, encIndex, int64(len(testContent)))

		if err == nil {
			t.Error("uploadEncryptedData() should return error when FinishUpload fails")
		}

		if !strings.Contains(err.Error(), "failed to finish upload") {
			t.Errorf("error should contain 'failed to finish upload', got: %v", err)
		}
	})
}

// TestUploadThumbnailWithRetry tests the thumbnail upload retry logic
func TestUploadThumbnailWithRetry(t *testing.T) {
	t.Run("successful upload on first try", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{UUID: TestThumbUUID, URL: mockServer.URL() + TestThumbPath},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", TestThumbETag)
			w.WriteHeader(http.StatusOK)
		}

		mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := FinishUploadResp{ID: TestThumbFileID}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.thumbnailHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		err := uploadThumbnailWithRetry(context.Background(), cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)
		if err != nil {
			t.Fatalf("expected success, got error: %v", err)
		}
	})

	t.Run("successful upload after retries", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		var attemptCount int

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			if attemptCount < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{UUID: TestThumbUUID, URL: mockServer.URL() + TestThumbPath},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", TestThumbETag)
			w.WriteHeader(http.StatusOK)
		}

		mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := FinishUploadResp{ID: TestThumbFileID}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.thumbnailHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		err := uploadThumbnailWithRetry(context.Background(), cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)
		if err != nil {
			t.Fatalf("expected success after retries, got error: %v", err)
		}

		if attemptCount != 3 {
			t.Errorf("expected 3 attempts, got %d", attemptCount)
		}
	})

	t.Run("failure after all retries exhausted", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		var attemptCount int

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			w.WriteHeader(http.StatusInternalServerError)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		err := uploadThumbnailWithRetry(context.Background(), cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)
		if err == nil {
			t.Fatal("expected error after retries exhausted, got nil")
		}

		if !strings.Contains(err.Error(), "after 5 retries") {
			t.Errorf("expected error to mention retries, got: %v", err)
		}

		if attemptCount != 5 {
			t.Errorf("expected 5 attempts, got %d", attemptCount)
		}
	})

	t.Run("non-retryable error fails immediately", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		var attemptCount int

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found: 404"))
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		err := uploadThumbnailWithRetry(context.Background(), cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}

		if !strings.Contains(err.Error(), "non-retryable error") {
			t.Errorf("expected non-retryable error, got: %v", err)
		}

		if attemptCount != 1 {
			t.Errorf("expected 1 attempt for non-retryable error, got %d", attemptCount)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		err := uploadThumbnailWithRetry(ctx, cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)
		if err == nil {
			t.Fatal("expected error for cancelled context, got nil")
		}

		if !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("expected context canceled error, got: %v", err)
		}
	})

	t.Run("thumbnail API registration failure triggers retry", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		var thumbnailAttemptCount int

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{UUID: TestThumbUUID, URL: mockServer.URL() + TestThumbPath},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", TestThumbETag)
			w.WriteHeader(http.StatusOK)
		}

		mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := FinishUploadResp{ID: TestThumbFileID}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.thumbnailHandler = func(w http.ResponseWriter, r *http.Request) {
			thumbnailAttemptCount++
			if thumbnailAttemptCount < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		err := uploadThumbnailWithRetry(context.Background(), cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)
		if err != nil {
			t.Fatalf("expected success after retries, got error: %v", err)
		}

		if thumbnailAttemptCount != 3 {
			t.Errorf("expected 3 thumbnail API attempts, got %d", thumbnailAttemptCount)
		}
	})
}

// TestUploadThumbnailAsync tests the async thumbnail upload wrapper
func TestUploadThumbnailAsync(t *testing.T) {
	t.Run("async upload completes without blocking", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		done := make(chan struct{})

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{UUID: TestThumbUUID, URL: mockServer.URL() + TestThumbPath},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", TestThumbETag)
			w.WriteHeader(http.StatusOK)
		}

		mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := FinishUploadResp{ID: TestThumbFileID}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.thumbnailHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			close(done)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		thumbnailWG.Add(1)
		go uploadThumbnailAsync(context.Background(), cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("async upload did not complete in time")
		}
	})
}

// TestWaitForPendingThumbnails tests that WaitForPendingThumbnails blocks until thumbnails complete
func TestWaitForPendingThumbnails(t *testing.T) {
	t.Run("waits for pending uploads", func(t *testing.T) {
		mockServer := newMockMultiEndpointServer()
		defer mockServer.Close()

		uploadStarted := make(chan struct{})
		uploadComplete := make(chan struct{})

		mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
			close(uploadStarted)
			resp := StartUploadResp{
				Uploads: []UploadPart{
					{UUID: TestThumbUUID, URL: mockServer.URL() + TestThumbPath},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Header().Set("ETag", TestThumbETag)
			w.WriteHeader(http.StatusOK)
		}

		mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
			resp := FinishUploadResp{ID: TestThumbFileID}
			json.NewEncoder(w).Encode(resp)
		}

		mockServer.thumbnailHandler = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			close(uploadComplete)
		}

		cfg := newTestConfigWithSetup(mockServer.URL(), nil)

		thumbnailWG.Add(1)
		go uploadThumbnailAsync(context.Background(), cfg, TestThumbFileUUID, TestThumbType, TestValidPNG)

		<-uploadStarted

		WaitForPendingThumbnails()

		select {
		case <-uploadComplete:
		default:
			t.Fatal("WaitForPendingThumbnails returned before upload completed")
		}
	})
}

// TestUploadFileStreamAuto_EmptyFile tests that empty files skip S3 upload and only create metadata
func TestUploadFileStreamAuto_EmptyFile(t *testing.T) {
	mockServer := newMockMultiEndpointServer()
	defer mockServer.Close()

	startCalled := false
	transferCalled := false
	finishCalled := false

	mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
		startCalled = true
		t.Error("START handler should not be called for empty files")
		w.WriteHeader(http.StatusBadRequest)
	}

	mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
		transferCalled = true
		t.Error("TRANSFER handler should not be called for empty files")
		w.WriteHeader(http.StatusBadRequest)
	}

	mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
		finishCalled = true
		t.Error("FINISH handler should not be called for empty files")
		w.WriteHeader(http.StatusBadRequest)
	}

	metaCalled := false
	var capturedFileID *string
	var capturedSize int64

	mockServer.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
		metaCalled = true
		var req CreateMetaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode CreateMetaRequest: %v", err)
		}

		capturedFileID = req.FileID
		capturedSize = req.Size

		resp := CreateMetaResponse{
			UUID:   "empty-file-uuid",
			FileID: "",
			Name:   "empty",
			Type:   "txt",
			Size:   "0",
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}

	cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
		c.Bucket = "test-bucket-empty"
	})

	emptyReader := bytes.NewReader([]byte{})
	result, err := UploadFileStreamAuto(context.Background(), cfg, TestFolderUUID, "empty.txt", emptyReader, 0, time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if !metaCalled {
		t.Error("CreateMetaFile handler should have been called")
	}
	if startCalled {
		t.Error("START handler was called but should have been skipped")
	}
	if transferCalled {
		t.Error("TRANSFER handler was called but should have been skipped")
	}
	if finishCalled {
		t.Error("FINISH handler was called but should have been skipped")
	}

	if capturedFileID != nil {
		t.Errorf("expected fileID to be nil, got %v", *capturedFileID)
	}

	if capturedSize != 0 {
		t.Errorf("expected size 0, got %d", capturedSize)
	}
}

// TestUploadFileStreamAuto_EmptyFile_UnknownSize tests empty file upload when size is unknown initially
func TestUploadFileStreamAuto_EmptyFile_UnknownSize(t *testing.T) {
	mockServer := newMockMultiEndpointServer()
	defer mockServer.Close()

	uploadsCalled := false

	mockServer.startHandler = func(w http.ResponseWriter, r *http.Request) {
		uploadsCalled = true
		t.Error("Upload handlers should not be called for empty files")
		w.WriteHeader(http.StatusBadRequest)
	}

	mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
		uploadsCalled = true
		t.Error("Upload handlers should not be called for empty files")
		w.WriteHeader(http.StatusBadRequest)
	}

	mockServer.finishHandler = func(w http.ResponseWriter, r *http.Request) {
		uploadsCalled = true
		t.Error("Upload handlers should not be called for empty files")
		w.WriteHeader(http.StatusBadRequest)
	}

	metaCalled := false
	mockServer.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
		metaCalled = true
		var req CreateMetaRequest
		json.NewDecoder(r.Body).Decode(&req)

		if req.FileID != nil {
			t.Errorf("expected fileID to be nil for empty file, got %v", *req.FileID)
		}
		if req.Size != 0 {
			t.Errorf("expected size 0, got %d", req.Size)
		}

		resp := CreateMetaResponse{
			UUID: "empty-uuid",
			Size: "0",
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}

	cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
		c.Bucket = "test-bucket-empty-unknown"
	})

	emptyReader := bytes.NewReader([]byte{})
	result, err := UploadFileStreamAuto(context.Background(), cfg, TestFolderUUID, "empty-unknown.txt", emptyReader, -1, time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if !metaCalled {
		t.Error("CreateMetaFile should have been called")
	}
	if uploadsCalled {
		t.Error("Upload handlers should have been skipped for empty file")
	}
}

// TestUploadFileStream_EmptyFile tests empty file handling with various filenames
func TestUploadFileStream_EmptyFile_ViaStreamAuto(t *testing.T) {
	mockServer := newMockMultiEndpointServer()
	defer mockServer.Close()

	var capturedRequest *CreateMetaRequest

	mockServer.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
		var req CreateMetaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}
		capturedRequest = &req

		resp := CreateMetaResponse{
			UUID:   "test-uuid",
			Name:   req.Name,
			Type:   req.Type,
			Size:   json.Number(fmt.Sprintf("%d", req.Size)),
			FileID: "",
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}

	cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
		c.Bucket = "test-bucket-stream-empty"
	})

	testCases := []struct {
		name         string
		fileName     string
		expectedName string
		expectedType string
	}{
		{"simple empty file", "empty.txt", "empty", "txt"},
		{"empty file no extension", "emptyfile", "emptyfile", ""},
		{"empty hidden file", ".hidden", "", "hidden"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capturedRequest = nil

			emptyReader := bytes.NewReader([]byte{})
			result, err := UploadFileStreamAuto(context.Background(), cfg, TestFolderUUID, tc.fileName, emptyReader, 0, time.Now())

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result == nil {
				t.Fatal("expected result, got nil")
			}

			if capturedRequest == nil {
				t.Fatal("CreateMetaFile was not called")
			}

			if capturedRequest.PlainName != tc.expectedName {
				t.Errorf("expected name '%s', got '%s'", tc.expectedName, capturedRequest.PlainName)
			}
			if capturedRequest.Type != tc.expectedType {
				t.Errorf("expected type '%s', got '%s'", tc.expectedType, capturedRequest.Type)
			}

			if capturedRequest.FileID != nil {
				t.Errorf("expected fileID to be nil, got %v", *capturedRequest.FileID)
			}
			if capturedRequest.Size != 0 {
				t.Errorf("expected size 0, got %d", capturedRequest.Size)
			}
		})
	}
}
>>>>>>> add-openapi-schema
