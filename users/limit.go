package users

import (
	"context"
	"fmt"

	"github.com/internxt/rclone-adapter/schema"
)

// GetLimit calls GET {DRIVE_API_URL}/users/limit and returns the maximum available storage of the account.
func GetLimit(ctx context.Context, client *schema.Client) (*schema.GetUserLimitDto, error) {
	resp, err := client.UserControllerLimit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get limit: %w", err)
	}

	parsed, err := schema.ParseUserControllerLimitResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if parsed.JSON200 != nil {
		return parsed.JSON200, nil
	}

	return nil, fmt.Errorf("unexpected response status: %d", resp.StatusCode)
}
