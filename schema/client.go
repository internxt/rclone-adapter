package schema

import (
	"context"
	"net/http"

	"github.com/internxt/rclone-adapter/config"
)

func NewOpenapiClient(baseUrl, token string) (*Client, error) {
	return NewClient(baseUrl,
		WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("internxt-client", config.ClientName)
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		}),
	)
}
