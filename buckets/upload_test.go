package buckets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
