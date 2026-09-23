package folders

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

func TestRenameFolder(t *testing.T) {
	t.Run("successful rename", func(t *testing.T) {
		var capturedPayload map[string]string

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("expected PUT request, got %s", r.Method)
			}

			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				t.Error("expected Authorization header with Bearer token")
			}

			if !strings.Contains(r.URL.Path, "test-uuid") || !strings.Contains(r.URL.Path, "/meta") {
				t.Errorf("expected path to contain test-uuid and /meta, got %s", r.URL.Path)
			}

			if err := json.NewDecoder(r.Body).Decode(&capturedPayload); err != nil {
				t.Errorf("failed to decode request body: %v", err)
			}

			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := RenameFolder(context.Background(), cfg, "test-uuid", "new-folder-name")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedPayload["plainName"] != "new-folder-name" {
			t.Errorf("expected plainName new-folder-name, got %s", capturedPayload["plainName"])
		}
	})

	t.Run("error - 404 not found", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := RenameFolder(context.Background(), cfg, "non-existent-uuid", "new-name")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain 404, got %v", err)
		}
	})
}

func TestMoveFolder(t *testing.T) {
	t.Run("successful move with rename", func(t *testing.T) {
		var capturedPayload map[string]string

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PATCH" {
				t.Errorf("expected PATCH request, got %s", r.Method)
			}

			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				t.Error("expected Authorization header with Bearer token")
			}

			if !strings.Contains(r.URL.Path, "test-uuid") {
				t.Errorf("expected path to contain test-uuid, got %s", r.URL.Path)
			}

			if err := json.NewDecoder(r.Body).Decode(&capturedPayload); err != nil {
				t.Errorf("failed to decode request body: %v", err)
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{}"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := MoveFolder(context.Background(), cfg, "test-uuid", "dest-folder-uuid", "new-name")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedPayload["destinationFolder"] != "dest-folder-uuid" {
			t.Errorf("expected destinationFolder dest-folder-uuid, got %s", capturedPayload["destinationFolder"])
		}
		if capturedPayload["name"] != "new-name" {
			t.Errorf("expected name new-name, got %s", capturedPayload["name"])
		}
	})

	t.Run("successful move without rename", func(t *testing.T) {
		var capturedPayload map[string]string

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := json.NewDecoder(r.Body).Decode(&capturedPayload); err != nil {
				t.Errorf("failed to decode request body: %v", err)
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{}"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := MoveFolder(context.Background(), cfg, "test-uuid", "dest-folder-uuid", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, ok := capturedPayload["name"]; ok {
			t.Error("expected name field to be omitted when empty, but it was present")
		}
	})

	t.Run("error - 404 not found", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		err := MoveFolder(context.Background(), cfg, "non-existent-uuid", "dest-folder-uuid", "")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain 404, got %v", err)
		}
	})
}

// cursorPage writes the page of items that starts at the offset encoded in the
// request cursor, with a nextCursor pointing to the following page.
func cursorPage[T any](w http.ResponseWriter, r *http.Request, key string, items []T, pageSize int) {
	start := 0
	if c := r.URL.Query().Get("cursor"); c != "" {
		start, _ = strconv.Atoi(c)
	}
	end := min(start+pageSize, len(items))
	var next *string
	if end < len(items) {
		n := strconv.Itoa(end)
		next = &n
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{key: items[start:end], "nextCursor": next})
}

func TestListFolders(t *testing.T) {
	t.Run("successful list with default values", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected GET request, got %s", r.Method)
			}
			if r.URL.Path != "/drive/folders/v2/content/parent-uuid/folders" {
				t.Errorf("unexpected path %s", r.URL.Path)
			}

			query := r.URL.Query()
			if query.Has("offset") || query.Has("sort") {
				t.Errorf("expected no offset/sort params, got %s", r.URL.RawQuery)
			}
			if query.Has("cursor") {
				t.Errorf("expected no cursor on first page, got %s", query.Get("cursor"))
			}
			if query.Get("limit") != "1000" {
				t.Errorf("expected limit 1000, got %s", query.Get("limit"))
			}
			if query.Get("order") != "ASC" {
				t.Errorf("expected order ASC, got %s", query.Get("order"))
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"folders":[{"uuid":"folder-1","plainName":"folder1"},{"uuid":"folder-2","plainName":"folder2"}],"nextCursor":"abc"}`))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		folders, next, err := ListFolders(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(folders) != 2 {
			t.Errorf("expected 2 folders, got %d", len(folders))
		}
		if next != "abc" {
			t.Errorf("expected next cursor abc, got %q", next)
		}
	})

	t.Run("successful list with custom options", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query()
			if query.Get("cursor") != "abc" {
				t.Errorf("expected cursor abc, got %s", query.Get("cursor"))
			}
			if query.Get("limit") != "75" {
				t.Errorf("expected limit 75, got %s", query.Get("limit"))
			}
			if query.Get("order") != "DESC" {
				t.Errorf("expected order DESC, got %s", query.Get("order"))
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"folders":[{"uuid":"folder-1"}],"nextCursor":null}`))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		opts := ListOptions{
			Cursor: "abc",
			Limit:  75,
			Order:  "DESC",
		}

		_, next, err := ListFolders(context.Background(), cfg, "parent-uuid", opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if next != "" {
			t.Errorf("expected empty next cursor on last page, got %q", next)
		}
	})

	t.Run("error - 500 server error", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, _, err := ListFolders(context.Background(), cfg, "parent-uuid", ListOptions{})
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

		_, _, err := ListFolders(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err == nil {
			t.Fatal("expected error for invalid JSON, got nil")
		}
		if !strings.Contains(err.Error(), "failed to decode") {
			t.Errorf("expected error to contain 'failed to decode', got %v", err)
		}
	})

	t.Run("error - missing folders array", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"nextCursor":null}`))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, _, err := ListFolders(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err == nil {
			t.Fatal("expected error for missing folders array, got nil")
		}
	})
}

func TestListFiles(t *testing.T) {
	t.Run("successful list with JSON numbers", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/drive/folders/v2/content/parent-uuid/files" {
				t.Errorf("unexpected path %s", r.URL.Path)
			}
			response := struct {
				Files      []File  `json:"files"`
				NextCursor *string `json:"nextCursor"`
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

		files, next, err := ListFiles(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(files) != 1 {
			t.Errorf("expected 1 file, got %d", len(files))
		}
		if files[0].UUID != "file-1" {
			t.Errorf("expected UUID file-1, got %s", files[0].UUID)
		}
		if next != "" {
			t.Errorf("expected empty next cursor, got %q", next)
		}
	})

	t.Run("error - 404 not found", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, _, err := ListFiles(context.Background(), cfg, "non-existent-uuid", ListOptions{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain 404, got %v", err)
		}
	})

	t.Run("error - missing files array", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"files":null,"nextCursor":null}`))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, _, err := ListFiles(context.Background(), cfg, "parent-uuid", ListOptions{})
		if err == nil {
			t.Fatal("expected error for missing files array, got nil")
		}
	})
}

func TestListAllFiles(t *testing.T) {
	t.Run("follows the cursor across multiple pages", func(t *testing.T) {
		all := make([]File, 2500)
		for i := range all {
			all[i] = File{UUID: "file-" + strconv.Itoa(i)}
		}
		var cursors []string
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cursors = append(cursors, r.URL.Query().Get("cursor"))
			if r.URL.Query().Get("limit") != "1000" {
				t.Errorf("expected limit 1000, got %s", r.URL.Query().Get("limit"))
			}
			cursorPage(w, r, "files", all, 1000)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		files, err := ListAllFiles(context.Background(), cfg, "parent-uuid")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(files) != len(all) {
			t.Errorf("expected %d files, got %d", len(all), len(files))
		}
		if files[len(files)-1].UUID != "file-2499" {
			t.Errorf("expected last file file-2499, got %s", files[len(files)-1].UUID)
		}
		if strings.Join(cursors, ",") != ",1000,2000" {
			t.Errorf("expected cursors [\"\" 1000 2000], got %q", cursors)
		}
	})

	t.Run("single page makes a single request", func(t *testing.T) {
		calls := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"files":[{"uuid":"file-1"},{"uuid":"file-2"}],"nextCursor":null}`))
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
		if calls != 1 {
			t.Errorf("expected 1 request, got %d", calls)
		}
	})

	t.Run("repeated cursor stops with an error", func(t *testing.T) {
		calls := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"files":[{"uuid":"file-1"}],"nextCursor":"same"}`))
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		_, err := ListAllFiles(context.Background(), cfg, "parent-uuid")
		if err == nil {
			t.Fatal("expected error for repeated cursor, got nil")
		}
		if calls != 2 {
			t.Errorf("expected 2 requests, got %d", calls)
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
	t.Run("follows the cursor across multiple pages", func(t *testing.T) {
		all := make([]Folder, 1200)
		for i := range all {
			all[i] = Folder{UUID: "folder-" + strconv.Itoa(i)}
		}
		calls := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			cursorPage(w, r, "folders", all, 1000)
		}))
		defer mockServer.Close()

		cfg := newTestConfig(mockServer.URL)

		folders, err := ListAllFolders(context.Background(), cfg, "parent-uuid")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(folders) != len(all) {
			t.Errorf("expected %d folders, got %d", len(all), len(folders))
		}
		if calls != 2 {
			t.Errorf("expected 2 requests, got %d", calls)
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

func TestListURLLimit(t *testing.T) {
	cases := []struct {
		limit int
		want  string
	}{
		{-5, "1000"},
		{0, "1000"},
		{10, "50"},
		{5000, "1000"},
	}
	for _, tc := range cases {
		u, err := listURL("https://example.com/list", ListOptions{Limit: tc.limit})
		if err != nil {
			t.Fatalf("limit %d: unexpected error: %v", tc.limit, err)
		}
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("limit %d: invalid URL %q: %v", tc.limit, u, err)
		}
		if got := parsed.Query().Get("limit"); got != tc.want {
			t.Errorf("limit %d: expected %s, got %s", tc.limit, tc.want, got)
		}
	}
}
