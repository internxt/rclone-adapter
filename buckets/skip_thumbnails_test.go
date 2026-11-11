package buckets

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/config"
)

// TestUploadFileStreamAutoSkipThumbnails checks that SkipThumbnails stops
// a thumbnail being made and uploaded for an image.
func TestUploadFileStreamAutoSkipThumbnails(t *testing.T) {
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, image.NewGray(image.Rect(0, 0, 64, 64)), nil); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip=%v", skip), func(t *testing.T) {
			mockServer := newMockMultiEndpointServer()
			defer mockServer.Close()
			mockServer.SetupSuccessfulUploadMock()
			transferHandler := mockServer.transferHandler
			mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				transferHandler(w, r)
			}
			var registered atomic.Int32
			mockServer.thumbnailHandler = func(w http.ResponseWriter, r *http.Request) {
				registered.Add(1)
				w.WriteHeader(http.StatusCreated)
			}
			cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
				c.HTTPClient = &http.Client{}
				c.SkipThumbnails = skip
			})

			_, err := UploadFileStreamAuto(context.Background(), cfg, TestFolderUUID, "img.jpg", bytes.NewReader(data), int64(len(data)), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			WaitForPendingThumbnails()

			want := int32(1)
			if skip {
				want = 0
			}
			if got := registered.Load(); got != want {
				t.Errorf("registered %d thumbnails, want %d", got, want)
			}
		})
	}
}
