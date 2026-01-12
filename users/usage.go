package users

import (
	"context"
	"fmt"

	"github.com/internxt/rclone-adapter/schema"
)

// GetUsage calls GET {DRIVE_API_URL}/users/usage and returns the account's current usage in bytes.
func GetUsage(ctx context.Context, client *schema.Client) (*schema.GetUserUsageDto, error) {
	resp, err := client.UserControllerGetUserUsage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get usage: %w", err)
	}

	parsed, err := schema.ParseUserControllerGetUserUsageResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse get usage response: %w", err)
	}

	if parsed.JSON200 != nil {
		return parsed.JSON200, nil
	}

	return nil, fmt.Errorf("unexpected get usage response status: %d", resp.StatusCode)
}
