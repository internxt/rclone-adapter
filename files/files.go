package files

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/internxt/rclone-adapter/config"
)

const filesPath = "/files"

// DeleteFile deletes a file by UUID
func DeleteFile(cfg *config.Config, uuid string) error {
	u, err := url.Parse(cfg.DriveAPIURL + filesPath + "/" + uuid)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DeleteFile failed: %d %s", resp.StatusCode, string(body))
	}

	return nil
}

// RenameFile renames a file by UUID with the given new name and optional type.
func RenameFile(cfg *config.Config, fileUUID, newPlainName, newType string) error {
	endpoint := cfg.DriveAPIURL + filesPath + "/" + fileUUID + "/meta"

	payload := map[string]string{
		"plainName": newPlainName,
	}
	if newType != "" {
		payload["type"] = newType
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("RenameFile failed: %d %s", resp.StatusCode, string(respBody))
	}

	return nil
}
