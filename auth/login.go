// auth/login.go
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/internxt/rclone-adapter/config"
)

type LoginResponse struct {
	HasKeys      bool   `json:"hasKeys"`
	SKey         string `json:"sKey"`
	TFA          bool   `json:"tfa"`
	HasKyberKeys bool   `json:"hasKyberKeys"`
	HasECCKeys   bool   `json:"hasEccKeys"`
}

// Login calls the auth login endpoint with {"email":…}
func Login(ctx context.Context, cfg *config.Config) (*LoginResponse, error) {
	payload := map[string]string{"email": cfg.Email}
	b, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Endpoints.Drive().Auth().Login(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var lr LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return nil, err
	}
	return &lr, nil
}
