package buckets

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"math/rand"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/internxt/rclone-adapter/config"
)

// TestUploadFileStreamAutoThumbnailMemory checks that uploads waiting for
// their thumbnail to be uploaded don't hold on to the file data.
func TestUploadFileStreamAutoThumbnailMemory(t *testing.T) {
	img := image.NewGray(image.Rect(0, 0, 2000, 1500))
	rand.New(rand.NewSource(1)).Read(img.Pix)
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	mockServer := newMockMultiEndpointServer()
	defer mockServer.Close()
	mockServer.SetupSuccessfulUploadMock()
	transferHandler := mockServer.transferHandler
	mockServer.transferHandler = func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		transferHandler(w, r)
	}
	release := make(chan struct{})
	registered := make(chan struct{}, 100)
	mockServer.thumbnailHandler = func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusCreated)
		registered <- struct{}{}
	}
	cfg := newTestConfigWithSetup(mockServer.URL(), func(c *config.Config) {
		c.HTTPClient = &http.Client{}
	})

	heapInUse := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heapInUse()

	const uploads = 20
	for i := range uploads {
		_, err := UploadFileStreamAuto(context.Background(), cfg, TestFolderUUID, fmt.Sprintf("img%d.jpg", i), bytes.NewReader(data), int64(len(data)), time.Now())
		if err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
	}
	grown := int64(heapInUse()) - int64(before)
	close(release)
	WaitForPendingThumbnails()

	if len(registered) != uploads {
		t.Errorf("registered %d thumbnails, want %d", len(registered), uploads)
	}
	if limit := int64(uploads * len(data) / 4); grown > limit {
		t.Errorf("heap grew by %d bytes with %d thumbnails pending, want at most %d (file size %d)", grown, uploads, limit, len(data))
	}
}
