package thumbnails

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/disintegration/imaging"
)

// supportedFormats maps file extensions to whether they support thumbnail generation
var supportedFormats = map[string]bool{
	"jpg":  true,
	"jpeg": true,
	"png":  true,
	"webp": true,
	"gif":  true,
	"tiff": true,
	"tif":  true,
}

// IsSupportedFormat checks if the given file extension supports thumbnail generation
func IsSupportedFormat(ext string) bool {
	normalized := strings.ToLower(strings.TrimPrefix(ext, "."))
	return supportedFormats[normalized]
}

// Generate creates a thumbnail from the provided image data.
// It resizes the image to fit within maxWidth x maxHeight while preserving aspect ratio,
// and returns the thumbnail as PNG bytes.
func Generate(imageData []byte, cfg *Config) ([]byte, int64, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	img, err := imaging.Decode(bytes.NewReader(imageData))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode image: %w", err)
	}

	thumb := imaging.Fit(img, cfg.MaxWidth, cfg.MaxHeight, imaging.Lanczos)

	var buf bytes.Buffer
	if err := imaging.Encode(&buf, thumb, imaging.PNG); err != nil {
		return nil, 0, fmt.Errorf("failed to encode thumbnail: %w", err)
	}

	thumbnailBytes := buf.Bytes()
	return thumbnailBytes, int64(len(thumbnailBytes)), nil
}
