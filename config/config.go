package config

import (
	"net"
	"net/http"
	"time"

	"github.com/internxt/rclone-adapter/endpoints"
)

const (
	DefaultDriveAPIURL      = "https://api.internxt.com/drive"
	DefaultAuthAPIURL       = "https://api.internxt.com/drive/auth"
	DefaultUsersAPIURL      = "https://api.internxt.com/users"
	DefaultAppCryptoSecret  = "6KYQBP847D4ATSFA"
	DefaultAppCryptoSecret2 = "8Q8VMUE3BJZV87GT"
	DefaultAppMagicIV       = "d139cb9a2cd17092e79e1861cf9d7023"
	DefaultAppMagicSalt     = "38dce0391b49efba88dbc8c39ebf868f0267eb110bb0012ab27dc52a528d61b1d1ed9d76f400ff58e3240028442b1eab9bb84e111d9dadd997982dbde9dbd25e"
	DefaultChunkSize        = 30 * 1024 * 1024
	DefaultMultipartMinSize = 100 * 1024 * 1024
	DefaultMaxConcurrency   = 6
	ClientName              = "rclone"
)

type Config struct {
	Token              string            `json:"token,omitempty"`
	RootFolderID       string            `json:"root_folder_id,omitempty"`
	Bucket             string            `json:"bucket,omitempty"`
	Mnemonic           string            `json:"mnemonic,omitempty"`
	BasicAuthHeader    string            `json:"basic_auth_header,omitempty"`
	DriveAPIURL        string            `json:"drive_api_url,omitempty"` // Deprecated: Use Endpoints instead
	AuthAPIURL         string            `json:"auth_api_url,omitempty"`  // Deprecated: Use Endpoints instead
	UsersAPIURL        string            `json:"users_api_url,omitempty"` // Deprecated: Use Endpoints instead
	AppCryptoSecret    string            `json:"app_crypto_secret,omitempty"`
	AppCryptoSecret2   string            `json:"app_crypto_secret2,omitempty"`
	AppMagicIV         string            `json:"app_magic_iv,omitempty"`
	AppMagicSalt       string            `json:"app_magic_salt,omitempty"`
	EncryptedPassword  string            `json:"encrypted_password,omitempty"`
	HTTPClient         *http.Client      `json:"-"` // Centralized HTTP client with proper timeouts
	Endpoints          *endpoints.Config `json:"-"` // Centralized API endpoint management
	SkipHashValidation bool              `json:"skip_hash_validation,omitempty"`
}

func NewDefaultToken(token string) *Config {
	cfg := &Config{
		Token: token,
	}
	cfg.ApplyDefaults()
	return cfg
}

// ApplyDefaults sets default values for any unset configuration fields.
// This is useful for test configurations to ensure they have properly configured HTTPClient with custom transport.
func (c *Config) ApplyDefaults() {
	if c.DriveAPIURL == "" {
		c.DriveAPIURL = DefaultDriveAPIURL
	}
	if c.AuthAPIURL == "" {
		c.AuthAPIURL = DefaultAuthAPIURL
	}
	if c.UsersAPIURL == "" {
		c.UsersAPIURL = DefaultUsersAPIURL
	}
	if c.AppCryptoSecret == "" {
		c.AppCryptoSecret = DefaultAppCryptoSecret
	}
	if c.AppCryptoSecret2 == "" {
		c.AppCryptoSecret2 = DefaultAppCryptoSecret2
	}
	if c.AppMagicIV == "" {
		c.AppMagicIV = DefaultAppMagicIV
	}
	if c.AppMagicSalt == "" {
		c.AppMagicSalt = DefaultAppMagicSalt
	}
	if c.HTTPClient == nil {
		c.HTTPClient = newHTTPClient()
	}
	if c.Endpoints == nil {
		c.Endpoints = endpoints.Default()
	}
}

// clientHeaderTransport wraps http.RoundTripper to automatically add the internxt-client header
type clientHeaderTransport struct {
	base http.RoundTripper
}

func (t *clientHeaderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("internxt-client", ClientName)
	return t.base.RoundTrip(req)
}

// newHTTPClient: properly configured HTTP client with sensible timeouts
func newHTTPClient() *http.Client {
	baseTransport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     50,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    false,
		ForceAttemptHTTP2:     true,
	}

	return &http.Client{
		Timeout:   5 * time.Minute,
		Transport: &clientHeaderTransport{base: baseTransport},
	}
}
