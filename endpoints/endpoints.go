package endpoints

import (
	"fmt"
	"net/url"
	"strings"
)

// Config holds the base URL configuration for all API endpoints
type Config struct {
	BaseURL string
}

// Default returns the production endpoints configuration
func Default() *Config {
	return &Config{
		BaseURL: "https://gateway.internxt.com",
	}
}

// NewConfig creates a new endpoints config with a custom base URL
func NewConfig(baseURL string) *Config {
	return &Config{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
	}
}

// driveURL returns the base drive API URL
func (c *Config) driveURL() string {
	return c.BaseURL + "/drive"
}

// Drive returns a DriveEndpoints helper for /drive/* endpoints
func (c *Config) Drive() *DriveEndpoints {
	return &DriveEndpoints{base: c.driveURL()}
}

func (c *Config) networkURL() string {
	return c.BaseURL + "/network"
}

// Network returns a NetworkEndpoints helper for /network/* endpoints
func (c *Config) Network() *NetworkEndpoints {
	return &NetworkEndpoints{base: c.networkURL()}
}

// DriveEndpoints provides endpoints under /drive
type DriveEndpoints struct {
	base string
}

// Auth returns auth-related endpoints
func (d *DriveEndpoints) Auth() *AuthEndpoints {
	return &AuthEndpoints{base: d.base + "/auth"}
}

// Files returns file-related endpoints
func (d *DriveEndpoints) Files() *FileEndpoints {
	return &FileEndpoints{base: d.base + "/files"}
}

// Folders returns folder-related endpoints
func (d *DriveEndpoints) Folders() *FolderEndpoints {
	return &FolderEndpoints{base: d.base + "/folders"}
}

// Trash returns trash-related endpoints
func (d *DriveEndpoints) Trash() *TrashEndpoints {
	return &TrashEndpoints{base: d.base + "/storage/trash"}
}

// Users returns user-related endpoints
func (d *DriveEndpoints) Users() *UserEndpoints {
	return &UserEndpoints{base: d.base + "/users"}
}

// Workspaces returns the workspaces endpoint
func (d *DriveEndpoints) Workspaces() string {
	return d.base + "/workspaces"
}

// FuzzySearch returns the fuzzy search endpoint
func (d *DriveEndpoints) FuzzySearch(term string, offset int) string {
	return fmt.Sprintf("%s/fuzzy/%s?offset=%d", d.base, url.PathEscape(term), offset)
}

// AuthEndpoints : endpoints under /drive/auth
type AuthEndpoints struct {
	base string
}

func (a *AuthEndpoints) Login() string       { return a.base + "/login" }
func (a *AuthEndpoints) LoginAccess() string { return a.base + "/login/access" }
func (a *AuthEndpoints) Logout() string      { return a.base + "/logout" }
func (a *AuthEndpoints) TFA() string         { return a.base + "/tfa" }

func (a *AuthEndpoints) CredentialsCorrect(hashedPassword string) string {
	return fmt.Sprintf("%s/are-credentials-correct?hashedPassword=%s", a.base, url.QueryEscape(hashedPassword))
}

// FileEndpoints : endpoints under /drive/files
type FileEndpoints struct {
	base string
}

func (f *FileEndpoints) Create() string            { return f.base }
func (f *FileEndpoints) Recents() string           { return f.base + "/recents" }
func (f *FileEndpoints) Meta(uuid string) string   { return f.base + "/" + uuid + "/meta" }
func (f *FileEndpoints) Delete(uuid string) string { return f.base + "/" + uuid }
func (f *FileEndpoints) Move(uuid string) string   { return f.base + "/" + uuid }
func (f *FileEndpoints) ByUUID(uuid string) string { return f.base + "/" + uuid }

// FolderEndpoints : endpoints under /drive/folders
type FolderEndpoints struct {
	base string
}

func (f *FolderEndpoints) Create() string               { return f.base }
func (f *FolderEndpoints) Delete(uuid string) string    { return f.base + "/" + uuid }
func (f *FolderEndpoints) Size(uuid string) string      { return f.base + "/" + uuid + "/size" }
func (f *FolderEndpoints) Meta(uuid string) string      { return f.base + "/" + uuid + "/meta" }
func (f *FolderEndpoints) Move(uuid string) string      { return f.base + "/" + uuid }
func (f *FolderEndpoints) Tree(uuid string) string      { return f.base + "/" + uuid + "/tree" }
func (f *FolderEndpoints) Ancestors(uuid string) string { return f.base + "/" + uuid + "/ancestors" }
func (f *FolderEndpoints) MetadataByID(id int64) string {
	return fmt.Sprintf("%s/%d/metadata", f.base, id)
}
func (f *FolderEndpoints) ByUUID(uuid string) string { return f.base + "/" + uuid }

func (f *FolderEndpoints) MetadataByPath(path string) string {
	return fmt.Sprintf("%s/meta?path=%s", f.base, url.QueryEscape(path))
}

func (f *FolderEndpoints) ContentFolders(parentUUID string) string {
	return fmt.Sprintf("%s/content/%s/folders", f.base, parentUUID)
}

func (f *FolderEndpoints) ContentFiles(parentUUID string) string {
	return fmt.Sprintf("%s/content/%s/files", f.base, parentUUID)
}

// TrashEndpoints : endpoints under /drive/storage/trash
type TrashEndpoints struct {
	base string
}

func (t *TrashEndpoints) Paginated() string               { return t.base + "/paginated" }
func (t *TrashEndpoints) Add() string                     { return t.base + "/add" }
func (t *TrashEndpoints) DeleteAll() string               { return t.base + "/all" }
func (t *TrashEndpoints) DeleteAllRequest() string        { return t.base + "/all/request" }
func (t *TrashEndpoints) DeleteItems() string             { return t.base }
func (t *TrashEndpoints) DeleteFile(fileID string) string { return t.base + "/file/" + fileID }
func (t *TrashEndpoints) DeleteFolder(folderID int64) string {
	return fmt.Sprintf("%s/folder/%d", t.base, folderID)
}

// UserEndpoints : endpoints under /users
type UserEndpoints struct {
	base string
}

func (u *UserEndpoints) Usage() string { return u.base + "/usage" }
func (u *UserEndpoints) Limit() string { return u.base + "/limit" }

// NetworkEndpoints : endpoints under /buckets and /v2/buckets
type NetworkEndpoints struct {
	base string
}

func (b *NetworkEndpoints) FileInfo(bucketID, fileID string) string {
	return fmt.Sprintf("%s/buckets/%s/files/%s/info", b.base, bucketID, fileID)
}

func (b *NetworkEndpoints) StartUpload(bucketID string) string {
	return fmt.Sprintf("%s/v2/buckets/%s/files/start", b.base, bucketID)
}

func (b *NetworkEndpoints) FinishUpload(bucketID string) string {
	return fmt.Sprintf("%s/v2/buckets/%s/files/finish", b.base, bucketID)
}
