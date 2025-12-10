package buckets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
)

// mockMultiEndpointServer is an alias for MockMultiEndpointServer
type mockMultiEndpointServer = MockMultiEndpointServer

func newMockMultiEndpointServer() *mockMultiEndpointServer {
	return NewMockMultiEndpointServer()
}

// TestUploadFile tests the complete file upload workflow from a file path
func TestUploadFile(t *testing.T) {
	// Create a temporary test file
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test-file.txt")
	testContent := []byte("Hello, world! This is test content.")
	if err := os.WriteFile(testFilePath, testContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	testCases := []struct {
		name          string
		filePath      string
		setupMock     func(*mockMultiEndpointServer)
		expectError   bool
		errorContains string
		setupConfig   func(*config.Config)
	}{
		{
			name:     "successful upload",
			filePath: testFilePath,
			setupMock: func(m *mockMultiEndpointServer) {
				// StartUpload handler
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{
							{
								UUID: "part-uuid-123",
								URL:  m.URL() + "/upload/test-url",
								URLs: []string{m.URL() + "/upload/test-url"},
							},
						},
					}
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(resp)
				}

				// Transfer handler
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"test-etag-123\"")
					w.WriteHeader(http.StatusOK)
				}

				// FinishUpload handler
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := FinishUploadResp{
						ID:      TestFileID,
						Bucket:  TestBucket1,
						Index:   "test-index",
						Created: "2025-01-01T00:00:00Z",
					}
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(resp)
				}

				// CreateMetaFile handler
				m.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := CreateMetaResponse{
						UUID:   TestFileUUID,
						FileID: TestFileID,
						Name:   "test-file",
						Type:   "txt",
					}
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(resp)
				}
			},
			expectError: false,
		},
		{
			name:          "error - file not found",
			filePath:      filepath.Join(tmpDir, "nonexistent.txt"),
			setupMock:     func(m *mockMultiEndpointServer) {},
			expectError:   true,
			errorContains: "failed to read file",
		},
		{
			name:     "error - StartUpload fails",
			filePath: testFilePath,
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("start upload error"))
				}
			},
			expectError:   true,
			errorContains: "failed to start upload",
		},
		{
			name:     "error - Transfer fails",
			filePath: testFilePath,
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{
							{
								UUID: "part-uuid",
								URL:  m.URL() + "/upload/test",
							},
						},
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}
			},
			expectError:   true,
			errorContains: "failed to transfer file data",
		},
		{
			name:     "error - FinishUpload fails",
			filePath: testFilePath,
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{{UUID: "part-uuid", URL: m.URL() + "/upload/test"}},
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"etag\"")
					w.WriteHeader(http.StatusOK)
				}
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte("finish error"))
				}
			},
			expectError:   true,
			errorContains: "failed to finish upload",
		},
		{
			name:     "error - CreateMetaFile fails",
			filePath: testFilePath,
			setupMock: func(m *mockMultiEndpointServer) {
				m.startHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := StartUploadResp{
						Uploads: []UploadPart{{UUID: "part-uuid", URL: m.URL() + "/upload/test"}},
					}
					json.NewEncoder(w).Encode(resp)
				}
				m.transferHandler = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", "\"etag\"")
					w.WriteHeader(http.StatusOK)
				}
				m.finishHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := FinishUploadResp{ID: "file-id"}
					json.NewEncoder(w).Encode(resp)
				}
				m.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
					w.Write([]byte("forbidden"))
				}
			},
			expectError:   true,
			errorContains: "failed to create file metadata",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := newMockMultiEndpointServer()
			defer mockServer.Close()

			tc.setupMock(mockServer)

			cfg := &config.Config{
				Mnemonic:        TestMnemonic,
				Bucket:          TestBucket1,
				Token:           TestToken,
				BasicAuthHeader: TestBasicAuth,
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL()),
			}

			if tc.setupConfig != nil {
				tc.setupConfig(cfg)
			}

			result, err := UploadFile(context.Background(), cfg, tc.filePath, "folder-uuid-123", time.Now())

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
				m.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := CreateMetaResponse{UUID: "stream-uuid", FileID: "stream-file-id"}
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

			cfg := &config.Config{
				Mnemonic:        TestMnemonic,
				Bucket:          TestBucket2,
				Token:           TestToken,
				BasicAuthHeader: TestBasicAuth,
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL()),
			}

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
				m.createMetaHandler = func(w http.ResponseWriter, r *http.Request) {
					resp := CreateMetaResponse{UUID: "multipart-uuid", FileID: "multipart-file-id"}
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

			cfg := &config.Config{
				Mnemonic:        TestMnemonic,
				Bucket:          TestBucket3,
				Token:           TestToken,
				BasicAuthHeader: TestBasicAuth,
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL()),
			}

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

			cfg := &config.Config{
				Mnemonic:        TestMnemonic,
				Bucket:          TestBucket4,
				Token:           TestToken,
				BasicAuthHeader: TestBasicAuth,
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL()),
			}

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

// TestUploadFileInvalidMnemonic tests that invalid mnemonic still generates keys
// (BIP39 doesn't validate mnemonic strength, just uses it as entropy)
func TestUploadFileInvalidMnemonic(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test.txt")
	os.WriteFile(testFilePath, []byte("test"), 0644)

	// BIP39 doesn't reject "invalid" mnemonics so this won't error
	cfg := &config.Config{
		Mnemonic:        "invalid mnemonic phrase that is not standard",
		Bucket:          TestBucket5,
		Token:           TestToken,
		BasicAuthHeader: TestBasicAuth,
		HTTPClient:      &http.Client{Timeout: 1},
		Endpoints:       endpoints.NewConfig("http://localhost"),
	}

	_, err := UploadFile(context.Background(), cfg, testFilePath, TestFolderUUID, time.Now())
	if err == nil {
		t.Error("expected error due to no server, got nil")
	}
	// Will fail at network request since localhost:80 is not available
	if !strings.Contains(err.Error(), "failed to start upload") && !strings.Contains(err.Error(), "HTTP request failed") {
		t.Errorf("expected network error, got: %v", err)
	}
}

// TestUploadFileStreamContextCancellation tests context cancellation handling
func TestUploadFileStreamContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	cfg := &config.Config{
		Mnemonic:        TestMnemonic,
		Bucket:          TestBucket6,
		Token:           TestToken,
		BasicAuthHeader: TestBasicAuth,
		HTTPClient:      &http.Client{},
		Endpoints:       endpoints.NewConfig("http://localhost"),
	}

	content := []byte("test content")
	reader := bytes.NewReader(content)

	_, err := UploadFileStream(ctx, cfg, TestFolderUUID, "test.txt", reader, int64(len(content)), time.Now())
	if err == nil {
		t.Error("expected error due to cancelled context, got nil")
	}
}

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

			cfg := &config.Config{
				Mnemonic:        TestMnemonic,
				Bucket:          TestBucket7,
				Token:           TestToken,
				BasicAuthHeader: TestBasicAuth,
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(mockServer.URL()),
			}

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
