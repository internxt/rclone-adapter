package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/errors"
)

type AccessResponse struct {
	User struct {
		Email               string `json:"email"`
		UserID              string `json:"userId"`
		Mnemonic            string `json:"mnemonic"`
		PrivateKey          string `json:"privateKey"`
		PublicKey           string `json:"publicKey"`
		RevocateKey         string `json:"revocateKey"`
		RootFolderID        string `json:"rootFolderId"`
		Name                string `json:"name"`
		Lastname            string `json:"lastname"`
		UUID                string `json:"uuid"`
		Credit              int    `json:"credit"`
		CreatedAt           string `json:"createdAt"`
		Bucket              string `json:"bucket"`
		RegisterCompleted   bool   `json:"registerCompleted"`
		Teams               bool   `json:"teams"`
		Username            string `json:"username"`
		BridgeUser          string `json:"bridgeUser"`
		SharedWorkspace     bool   `json:"sharedWorkspace"`
		HasReferralsProgram bool   `json:"hasReferralsProgram"`
		BackupsBucket       string `json:"backupsBucket"`
		Avatar              string `json:"avatar"`
		EmailVerified       bool   `json:"emailVerified"`
		LastPasswordChanged string `json:"lastPasswordChangedAt"`
	} `json:"user"`
	Token    string          `json:"token"`
	UserTeam json.RawMessage `json:"userTeam"`
	NewToken string          `json:"newToken"`
}

func RefreshToken(ctx context.Context, cfg *config.Config) (*AccessResponse, error) {
	endpoint := cfg.Endpoints.Drive().Users().Refresh()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create refresh token request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute refresh token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.NewHTTPError(resp, "refresh token")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read refresh response: %w", err)
	}

	var ar AccessResponse
	if err := json.Unmarshal(body, &ar); err != nil {
		return nil, fmt.Errorf("failed to parse refresh response: %w", err)
	}

	if ar.NewToken == "" {
		return nil, fmt.Errorf("refresh response missing newToken")
	}

	return &ar, nil
}
