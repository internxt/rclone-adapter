package endpoints

import "testing"

func TestEndpointConstruction(t *testing.T) {
	cfg := Default()

	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"Auth Login", cfg.Drive().Auth().Login(), "https://gateway.internxt.com/drive/auth/login"},
		{"File Create", cfg.Drive().Files().Create(), "https://gateway.internxt.com/drive/files"},
		{"File Meta", cfg.Drive().Files().Meta("test-uuid"), "https://gateway.internxt.com/drive/files/test-uuid/meta"},
		{"File Delete", cfg.Drive().Files().Delete("test-uuid"), "https://gateway.internxt.com/drive/files/test-uuid"},
		{"Folder Create", cfg.Drive().Folders().Create(), "https://gateway.internxt.com/drive/folders"},
		{"Folder Delete", cfg.Drive().Folders().Delete("test-uuid"), "https://gateway.internxt.com/drive/folders/test-uuid"},
		{"Folder ContentFolders", cfg.Drive().Folders().ContentFolders("parent-uuid"), "https://gateway.internxt.com/drive/folders/content/parent-uuid/folders"},
		{"Folder ContentFiles", cfg.Drive().Folders().ContentFiles("parent-uuid"), "https://gateway.internxt.com/drive/folders/content/parent-uuid/files"},
		{"User Usage", cfg.Drive().Users().Usage(), "https://gateway.internxt.com/drive/users/usage"},
		{"User Limit", cfg.Drive().Users().Limit(), "https://gateway.internxt.com/drive/users/limit"},
		{"Network FileInfo", cfg.Network().FileInfo("bucket-123", "file-456"), "https://gateway.internxt.com/network/buckets/bucket-123/files/file-456/info"},
		{"Network StartUpload", cfg.Network().StartUpload("bucket-123"), "https://gateway.internxt.com/network/v2/buckets/bucket-123/files/start"},
		{"Network FinishUpload", cfg.Network().FinishUpload("bucket-123"), "https://gateway.internxt.com/network/v2/buckets/bucket-123/files/finish"},
		{"File Check Files Existence", cfg.Drive().Folders().CheckFilesExistence("parent-uuid"), "https://gateway.internxt.com/drive/folders/content/parent-uuid/files/existence"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s:\ngot:      %s\nexpected: %s", tt.name, tt.got, tt.expected)
			}
		})
	}
}
