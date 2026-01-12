package schema

import (
	"context"
	"net/http"
)

const (
	BaseUrl         = "https://gateway.internxt.com/drive"
	InternxtClient  = "rclone"
	InternxtVersion = "1.0"
)

func NewInternxtClient(token string) (*Client, error) {
	return NewClient(BaseUrl,
		WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("internxt-client", InternxtClient)
			req.Header.Set("internxt-version", InternxtVersion)
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		}),
	)
}
