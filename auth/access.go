package auth

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"crypto/sha1"

	"github.com/internxt/rclone-adapter/config"

	"golang.org/x/crypto/pbkdf2"
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

// AccessLogin calls {DRIVE_API_URL}/auth/login/access based on our previous LoginResponse
func AccessLogin(ctx context.Context, cfg *config.Config, lr *LoginResponse) (*AccessResponse, error) {
	encPwd, err := deriveEncryptedPassword(cfg.Password, lr.SKey, cfg.AppCryptoSecret)
	if err != nil {
		return nil, err
	}
	cfg.EncryptedPassword = encPwd

	req := map[string]interface{}{
		"email":    cfg.Email,
		"password": encPwd,
	}
	if lr.TFA && cfg.TFA != "" {
		req["tfa"] = cfg.TFA
	}

	b, _ := json.Marshal(req)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Endpoints.Drive().Auth().LoginAccess(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var ar AccessResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return nil, err
	}

	cfg.Token = ar.NewToken
	cfg.RootFolderID = ar.User.RootFolderID
	cfg.Bucket = ar.User.Bucket

	// 1) SHA256 the raw pass string
	sum := sha256.Sum256([]byte(ar.User.UserID))
	hexPass := hex.EncodeToString(sum[:])

	// 2) build "user:hexPass" and Base64
	creds := fmt.Sprintf("%s:%s", ar.User.BridgeUser, hexPass)
	cfg.BasicAuthHeader = "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))

	cfg.Mnemonic, err = decryptTextWithKey(ar.User.Mnemonic, cfg.Password)
	if err != nil {
		return nil, err
	}

	return &ar, nil
}

func AreCredentialsCorrect(ctx context.Context, cfg *config.Config, hashedPassword string) (bool, error) {
	endpoint := cfg.Endpoints.Drive().Auth().CredentialsCorrect(hashedPassword)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

func deriveEncryptedPassword(password, hexSalt, secret string) (string, error) {
	// decrypt the OpenSSL‐style salt blob to hex salt string
	saltHex, err := decryptTextWithKey(hexSalt, secret)
	if err != nil {
		return "", err
	}
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return "", err
	}
	// PBKDF2‐SHA1
	key := pbkdf2.Key([]byte(password), salt, 10000, 32, sha1.New)
	hashHex := hex.EncodeToString(key)

	// re‐encrypt with OpenSSL style AES‑CBC
	return encryptTextWithKey(hashHex, secret)
}

func decryptTextWithKey(hexCipher, secret string) (string, error) {
	data, err := hex.DecodeString(hexCipher)
	if err != nil {
		return "", fmt.Errorf("invalid hex encoding: %w", err)
	}

	if len(data) < 32 {
		return "", errors.New("ciphertext too short")
	}

	if string(data[:8]) != "Salted__" {
		return "", errors.New("invalid ciphertext format: missing Salted__ header")
	}

	salt := data[8:16]

	// EVP_BytesToKey with MD5 ×3
	d := append([]byte(secret), salt...)
	var prev = d
	hashes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		h := md5.Sum(prev)
		hashes[i] = h[:]
		prev = append(hashes[i], d...)
	}
	key := append(hashes[0], hashes[1]...)
	iv := hashes[2]

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	ct := data[16:]
	if len(ct)%aes.BlockSize != 0 {
		return "", errors.New("ciphertext is not a multiple of block size")
	}

	// Decrypt
	pt := make([]byte, len(ct))
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(pt, ct)

	// Validate and remove PKCS#7 padding
	if len(pt) == 0 {
		return "", errors.New("plaintext is empty")
	}

	padLen := int(pt[len(pt)-1])
	if padLen == 0 || padLen > aes.BlockSize {
		return "", errors.New("invalid PKCS#7 padding")
	}
	if padLen > len(pt) {
		return "", errors.New("invalid PKCS#7 padding length")
	}

	paddingStart := len(pt) - padLen
	validPadding := byte(1)
	for i := paddingStart; i < len(pt); i++ {
		validPadding &= byte(1) ^ ((pt[i] ^ byte(padLen)) >> 7)
		validPadding &= byte(1) ^ (((pt[i] ^ byte(padLen)) << 7) >> 7)
		if pt[i] != byte(padLen) {
			validPadding = 0
		}
	}

	if validPadding == 0 {
		return "", errors.New("invalid PKCS#7 padding bytes")
	}

	return string(pt[:paddingStart]), nil
}

func encryptTextWithKey(plaintext, secret string) (string, error) {
	salt := make([]byte, 8)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate random salt: %w", err)
	}

	d := append([]byte(secret), salt...)
	var prev = d
	hashes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		h := md5.Sum(prev)
		hashes[i] = h[:]
		prev = append(hashes[i], d...)
	}
	key := append(hashes[0], hashes[1]...)
	iv := hashes[2]

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	// PKCS#7 padding
	plaintextBytes := []byte(plaintext)
	padLen := aes.BlockSize - len(plaintextBytes)%aes.BlockSize
	padding := make([]byte, padLen)
	for i := range padding {
		padding[i] = byte(padLen)
	}
	paddedPlaintext := append(plaintextBytes, padding...)

	ct := make([]byte, len(paddedPlaintext))
	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ct, paddedPlaintext)

	out := append([]byte("Salted__"), salt...)
	out = append(out, ct...)
	return hex.EncodeToString(out), nil
}

func RefreshToken(ctx context.Context, cfg *config.Config) (*AccessResponse, error) {
	endpoint := cfg.Endpoints.Drive().Users().Refresh()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read refresh response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh token failed with status %d", resp.StatusCode)
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
