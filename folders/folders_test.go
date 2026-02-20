package folders

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/consistency"
)

func TestCreateFolder(t *testing.T) {
	t.Run("successful creation with auto-filled timestamps", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected POST request, got %s", r.Method)
			}

			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
			}

			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				t.Error("expected Authorization header with Bearer token")
			}

			var reqBody CreateFolderRequest
			if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
				t.Errorf("failed to decode request body: %v", err)
			}

			if reqBody.PlainName != "test-folder" {
				t.Errorf("expected PlainName test-folder, got %s", reqBody.PlainName)
			}
			if reqBody.ParentFolderUUID != "parent-uuid" {
				t.Errorf("expected ParentFolderUUID parent-uuid, got %s", reqBody.ParentFolderUUID)
			}
			if reqBody.CreationTime == "" {
				t.Error("expected CreationTime to be auto-filled, got empty")
			}
			if reqBody.ModificationTime == "" {
				t.Error("expected ModificationTime to be auto-filled, got empty")
			}

			response := Folder{
				UUID:      "new-folder-uuid",
				PlainName: "test-folder",
				ID:        123,
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		reqBody := CreateFolderRequest{
			PlainName:        "test-folder",
			ParentFolderUUID: "parent-uuid",
		}

		folder, err := CreateFolder(context.Background(), cfg, reqBody)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if folder.UUID != "new-folder-uuid" {
			t.Errorf("expected UUID new-folder-uuid, got %s", folder.UUID)
		}
		if folder.PlainName != "test-folder" {
			t.Errorf("expected PlainName test-folder, got %s", folder.PlainName)
		}
	})

	t.Run("successful creation with provided timestamps", func(t *testing.T) {
		now := time.Now().UTC().Format(time.RFC3339Nano)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqBody CreateFolderRequest
			json.NewDecoder(r.Body).Decode(&reqBody)

			if reqBody.CreationTime != now {
				t.Errorf("expected CreationTime %s, got %s", now, reqBody.CreationTime)
			}
			if reqBody.ModificationTime != now {
				t.Errorf("expected ModificationTime %s, got %s", now, reqBody.ModificationTime)
			}

			response := Folder{UUID: "new-folder-uuid"}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		reqBody := CreateFolderRequest{
			PlainName:        "test-folder",
			ParentFolderUUID: "parent-uuid",
			CreationTime:     now,
			ModificationTime: now,
		}

		_, err := CreateFolder(context.Background(), cfg, reqBody)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("success with 201 status code", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := Folder{UUID: "new-folder-uuid"}
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		reqBody := CreateFolderRequest{
			PlainName:        "test-folder",
			ParentFolderUUID: "parent-uuid",
		}

		_, err := CreateFolder(context.Background(), cfg, reqBody)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("error - 401 unauthorized", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("unauthorized"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		reqBody := CreateFolderRequest{
			PlainName:        "test-folder",
			ParentFolderUUID: "parent-uuid",
		}

		_, err := CreateFolder(context.Background(), cfg, reqBody)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "401") {
			t.Errorf("expected error to contain 401, got %v", err)
		}
	})

	t.Run("error - invalid JSON response", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid json"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		reqBody := CreateFolderRequest{
			PlainName:        "test-folder",
			ParentFolderUUID: "parent-uuid",
		}

		_, err := CreateFolder(context.Background(), cfg, reqBody)
		if err == nil {
			t.Fatal("expected error for invalid JSON, got nil")
		}
		if !strings.Contains(err.Error(), "failed to decode") {
			t.Errorf("expected error to contain 'failed to decode', got %v", err)
		}
	})
}

func TestCreateFolderTracksConsistency(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := Folder{UUID: "tracked-uuid", PlainName: "test"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer mockServer.Close()

	cfg := newTestConfig(mockServer.URL)
	_, err := CreateFolder(context.Background(), cfg, CreateFolderRequest{
		PlainName:        "test",
		ParentFolderUUID: "parent-uuid",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// AwaitFolder should block briefly for a just-created folder
	start := time.Now()
	if err := consistency.AwaitFolder(context.Background(), "tracked-uuid"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if time.Since(start) < 400*time.Millisecond {
		t.Error("expected AwaitFolder to block for a recently created folder")
	}
}

func TestDeleteFolder(t *testing.T) {
	t.Run("successful deletion - 204", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected DELETE request, got %s", r.Method)
			}

			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				t.Error("expected Authorization header with Bearer token")
			}

			if !strings.Contains(r.URL.Path, "test-uuid") {
				t.Errorf("expected path to contain test-uuid, got %s", r.URL.Path)
			}

			w.WriteHeader(204)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := DeleteFolder(context.Background(), cfg, "test-uuid")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("error - 404 not found", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := DeleteFolder(context.Background(), cfg, "non-existent-uuid")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain 404, got %v", err)
		}
	})

	t.Run("error - 500 server error", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := DeleteFolder(context.Background(), cfg, "test-uuid")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "500") {
			t.Errorf("expected error to contain 500, got %v", err)
		}
	})
}

func TestListFolders(t *testing.T) {
	t.Run("successful list with default values", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected GET request, got %s", r.Method)
			}

			query := r.URL.Query()
			if query.Get("offset") != "0" {
				t.Errorf("expected offset 0, got %s", query.Get("offset"))
			}
			if query.Get("limit") != "50" {
				t.Errorf("expected limit 50, got %s", query.Get("limit"))
			}
			if query.Get("sort") != "plainName" {
				t.Errorf("expected sort plainName, got %s", query.Get("sort"))
			}
			if query.Get("order") != "ASC" {
				t.Errorf("expected order ASC, got %s", query.Get("order"))
			}

			response := struct {
				Folders []Folder `json:"folders"`
			}{
				Folders: []Folder{
					{UUID: "folder-1", PlainName: "folder1"},
					{UUID: "folder-2", PlainName: "folder2"},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		folders, err := ListFolders(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(folders) != 2 {
			t.Errorf("expected 2 folders, got %d", len(folders))
		}
	})

	t.Run("successful list with custom pagination", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query()
			if query.Get("offset") != "10" {
				t.Errorf("expected offset 10, got %s", query.Get("offset"))
			}
			if query.Get("limit") != "25" {
				t.Errorf("expected limit 25, got %s", query.Get("limit"))
			}
			if query.Get("sort") != "createdAt" {
				t.Errorf("expected sort createdAt, got %s", query.Get("sort"))
			}
			if query.Get("order") != "DESC" {
				t.Errorf("expected order DESC, got %s", query.Get("order"))
			}

			response := struct {
				Folders []Folder `json:"folders"`
			}{
				Folders: []Folder{{UUID: "folder-1"}},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		opts := ListOptions{
			Offset: 10,
			Limit:  25,
			Sort:   "createdAt",
			Order:  "DESC",
		}

		_, err := ListFolders(context.Background(), cfg, "parent-uuid", opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("default values for negative/zero limits", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query()
			if query.Get("limit") != "50" {
				t.Errorf("expected limit 50 (default), got %s", query.Get("limit"))
			}
			if query.Get("offset") != "0" {
				t.Errorf("expected offset 0 (default for negative), got %s", query.Get("offset"))
			}

			response := struct {
				Folders []Folder `json:"folders"`
			}{Folders: []Folder{}}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		opts := ListOptions{
			Limit:  -5,  // Should default to 50
			Offset: -10, // Should default to 0
		}

		_, err := ListFolders(context.Background(), cfg, "parent-uuid", opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("error - 500 server error", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, err := ListFolders(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "500") {
			t.Errorf("expected error to contain 500, got %v", err)
		}
	})

	t.Run("error - invalid JSON response", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid json"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, err := ListFolders(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err == nil {
			t.Fatal("expected error for invalid JSON, got nil")
		}
		if !strings.Contains(err.Error(), "failed to decode") {
			t.Errorf("expected error to contain 'failed to decode', got %v", err)
		}
	})
}

func TestListFiles(t *testing.T) {
	t.Run("successful list with JSON numbers", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := struct {
				Files []File `json:"files"`
			}{
				Files: []File{
					{
						UUID:      "file-1",
						PlainName: "file1.txt",
						Size:      json.Number("1024"),
						FolderID:  json.Number("123"),
						UserID:    json.Number("456"),
					},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		files, err := ListFiles(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(files) != 1 {
			t.Errorf("expected 1 file, got %d", len(files))
		}
		if files[0].UUID != "file-1" {
			t.Errorf("expected UUID file-1, got %s", files[0].UUID)
		}
	})

	t.Run("error - 404 not found", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, err := ListFiles(context.Background(), cfg, "non-existent-uuid", ListOptions{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain 404, got %v", err)
		}
	})
}

func TestListAllFiles(t *testing.T) {
	t.Run("pagination loop - multiple pages", func(t *testing.T) {
		callCount := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query()
			offset := 0
			if query.Get("offset") != "" {
				// Parse offset from query
				offset = callCount * 50
			}

			files := []File{}
			// Return 50 files for first two calls, then 10 for the last
			if callCount < 2 {
				for i := range 50 {
					files = append(files, File{UUID: "file-" + string(rune(offset+i))})
				}
			} else {
				for i := range 10 {
					files = append(files, File{UUID: "file-" + string(rune(offset+i))})
				}
			}

			response := struct {
				Files []File `json:"files"`
			}{Files: files}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
			callCount++
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		files, err := ListAllFiles(context.Background(), cfg, "parent-uuid")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should have 50 + 50 + 10 = 110 files
		if len(files) != 110 {
			t.Errorf("expected 110 files, got %d", len(files))
		}
	})

	t.Run("single page - less than 50 files", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := struct {
				Files []File `json:"files"`
			}{
				Files: []File{
					{UUID: "file-1"},
					{UUID: "file-2"},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		files, err := ListAllFiles(context.Background(), cfg, "parent-uuid")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(files) != 2 {
			t.Errorf("expected 2 files, got %d", len(files))
		}
	})

	t.Run("error handling", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, err := ListAllFiles(context.Background(), cfg, "parent-uuid")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "failed to list all files") {
			t.Errorf("expected error to contain 'failed to list all files', got %v", err)
		}
	})
}

func TestListAllFolders(t *testing.T) {
	t.Run("pagination loop - multiple pages", func(t *testing.T) {
		callCount := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			folders := []Folder{}
			// Return 50 folders for first call, then 25 for the second
			if callCount == 0 {
				for i := range 50 {
					folders = append(folders, Folder{UUID: "folder-" + string(rune(i))})
				}
			} else {
				for i := range 25 {
					folders = append(folders, Folder{UUID: "folder-" + string(rune(i+50))})
				}
			}

			response := struct {
				Folders []Folder `json:"folders"`
			}{Folders: folders}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
			callCount++
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		folders, err := ListAllFolders(context.Background(), cfg, "parent-uuid")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should have 50 + 25 = 75 folders
		if len(folders) != 75 {
			t.Errorf("expected 75 folders, got %d", len(folders))
		}
	})

	t.Run("error handling", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, err := ListAllFolders(context.Background(), cfg, "parent-uuid")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "failed to list all folders") {
			t.Errorf("expected error to contain 'failed to list all folders', got %v", err)
		}
	})
}
