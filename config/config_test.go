package config

import (
	"net/http"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/endpoints"
)

func TestNewDefaultToken(t *testing.T) {
	token := "test-token-123"

	cfg := NewDefaultToken(token)

	if cfg.Token != token {
		t.Errorf("expected Token %s, got %s", token, cfg.Token)
	}
	if cfg.DriveAPIURL != DefaultDriveAPIURL {
		t.Errorf("expected DriveAPIURL %s, got %s", DefaultDriveAPIURL, cfg.DriveAPIURL)
	}
	if cfg.HTTPClient == nil {
		t.Error("expected HTTPClient to be initialized, got nil")
	}
	if cfg.Endpoints == nil {
		t.Error("expected Endpoints to be initialized, got nil")
	}
}

func TestApplyDefaults(t *testing.T) {
	t.Run("all defaults applied", func(t *testing.T) {
		cfg := &Config{}
		cfg.ApplyDefaults()

		if cfg.DriveAPIURL != DefaultDriveAPIURL {
			t.Errorf("expected DriveAPIURL %s, got %s", DefaultDriveAPIURL, cfg.DriveAPIURL)
		}
		if cfg.AuthAPIURL != DefaultAuthAPIURL {
			t.Errorf("expected AuthAPIURL %s, got %s", DefaultAuthAPIURL, cfg.AuthAPIURL)
		}
		if cfg.UsersAPIURL != DefaultUsersAPIURL {
			t.Errorf("expected UsersAPIURL %s, got %s", DefaultUsersAPIURL, cfg.UsersAPIURL)
		}
		if cfg.AppCryptoSecret != DefaultAppCryptoSecret {
			t.Errorf("expected AppCryptoSecret %s, got %s", DefaultAppCryptoSecret, cfg.AppCryptoSecret)
		}
		if cfg.AppCryptoSecret2 != DefaultAppCryptoSecret2 {
			t.Errorf("expected AppCryptoSecret2 %s, got %s", DefaultAppCryptoSecret2, cfg.AppCryptoSecret2)
		}
		if cfg.AppMagicIV != DefaultAppMagicIV {
			t.Errorf("expected AppMagicIV %s, got %s", DefaultAppMagicIV, cfg.AppMagicIV)
		}
		if cfg.AppMagicSalt != DefaultAppMagicSalt {
			t.Errorf("expected AppMagicSalt %s, got %s", DefaultAppMagicSalt, cfg.AppMagicSalt)
		}
		if cfg.HTTPClient == nil {
			t.Error("expected HTTPClient to be initialized, got nil")
		}
		if cfg.Endpoints == nil {
			t.Error("expected Endpoints to be initialized, got nil")
		}
	})

	t.Run("preserves existing values", func(t *testing.T) {
		customDriveURL := "https://custom.drive.url"
		customAuthURL := "https://custom.auth.url"
		customSecret := "custom-secret"
		customClient := &http.Client{Timeout: 1 * time.Second}
		customEndpoints := endpoints.NewConfig("https://custom.base.url")

		cfg := &Config{
			DriveAPIURL:     customDriveURL,
			AuthAPIURL:      customAuthURL,
			AppCryptoSecret: customSecret,
			HTTPClient:      customClient,
			Endpoints:       customEndpoints,
		}
		cfg.ApplyDefaults()

		if cfg.DriveAPIURL != customDriveURL {
			t.Errorf("expected DriveAPIURL to be preserved as %s, got %s", customDriveURL, cfg.DriveAPIURL)
		}
		if cfg.AuthAPIURL != customAuthURL {
			t.Errorf("expected AuthAPIURL to be preserved as %s, got %s", customAuthURL, cfg.AuthAPIURL)
		}
		if cfg.AppCryptoSecret != customSecret {
			t.Errorf("expected AppCryptoSecret to be preserved as %s, got %s", customSecret, cfg.AppCryptoSecret)
		}
		if cfg.HTTPClient != customClient {
			t.Error("expected HTTPClient to be preserved, got different instance")
		}
		if cfg.Endpoints != customEndpoints {
			t.Error("expected Endpoints to be preserved, got different instance")
		}

		if cfg.UsersAPIURL != DefaultUsersAPIURL {
			t.Errorf("expected UsersAPIURL %s, got %s", DefaultUsersAPIURL, cfg.UsersAPIURL)
		}
		if cfg.AppCryptoSecret2 != DefaultAppCryptoSecret2 {
			t.Errorf("expected AppCryptoSecret2 %s, got %s", DefaultAppCryptoSecret2, cfg.AppCryptoSecret2)
		}
	})
}

func TestNewHTTPClient(t *testing.T) {
	client := newHTTPClient()

	if client == nil {
		t.Fatal("expected HTTPClient to be created, got nil")
	}

	if client.Timeout != 5*time.Minute {
		t.Errorf("expected Timeout 5 minutes, got %v", client.Timeout)
	}

	if client.Transport == nil {
		t.Fatal("expected Transport to be set, got nil")
	}

	// Transport is wrapped in clientHeaderTransport, so unwrap it
	headerTransport, ok := client.Transport.(*clientHeaderTransport)
	if !ok {
		t.Fatalf("expected Transport to be *clientHeaderTransport, got %T", client.Transport)
	}

	transport, ok := headerTransport.base.(*http.Transport)
	if !ok {
		t.Fatalf("expected base transport to be *http.Transport, got %T", headerTransport.base)
	}

	if transport.MaxIdleConns != 100 {
		t.Errorf("expected MaxIdleConns 100, got %d", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 10 {
		t.Errorf("expected MaxIdleConnsPerHost 10, got %d", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxConnsPerHost != 50 {
		t.Errorf("expected MaxConnsPerHost 50, got %d", transport.MaxConnsPerHost)
	}
	if transport.IdleConnTimeout != 90*time.Second {
		t.Errorf("expected IdleConnTimeout 90s, got %v", transport.IdleConnTimeout)
	}
	if transport.TLSHandshakeTimeout != 10*time.Second {
		t.Errorf("expected TLSHandshakeTimeout 10s, got %v", transport.TLSHandshakeTimeout)
	}
	if transport.ResponseHeaderTimeout != 20*time.Second {
		t.Errorf("expected ResponseHeaderTimeout 20s, got %v", transport.ResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout != 1*time.Second {
		t.Errorf("expected ExpectContinueTimeout 1s, got %v", transport.ExpectContinueTimeout)
	}
	if transport.DisableKeepAlives != false {
		t.Errorf("expected DisableKeepAlives false, got %v", transport.DisableKeepAlives)
	}
	if transport.DisableCompression != false {
		t.Errorf("expected DisableCompression false, got %v", transport.DisableCompression)
	}
	if transport.ForceAttemptHTTP2 != true {
		t.Errorf("expected ForceAttemptHTTP2 true, got %v", transport.ForceAttemptHTTP2)
	}

	if transport.DialContext == nil {
		t.Error("expected DialContext to be set, got nil")
	}
}
