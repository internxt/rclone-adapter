package thumbnails

import (
	"bytes"
	"image"
	"image/png"
	"testing"
)

func TestIsSupportedFormat(t *testing.T) {
	tests := []struct {
		ext      string
		expected bool
	}{
		{"jpg", true},
		{"JPG", true},
		{".jpg", true},
		{"jpeg", true},
		{".jpeg", true},
		{"png", true},
		{".png", true},
		{"webp", true},
		{"gif", true},
		{"tiff", true},
		{"tif", true},
		{"pdf", false},
		{"txt", false},
		{"mp4", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			result := IsSupportedFormat(tt.ext)
			if result != tt.expected {
				t.Errorf("IsSupportedFormat(%q) = %v, want %v", tt.ext, result, tt.expected)
			}
		})
	}
}

func TestGenerate(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 100, 100))
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			img.Set(x, y, image.Black)
		}
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("failed to encode test image: %v", err)
	}
	testImageData := buf.Bytes()

	t.Run("ValidImage", func(t *testing.T) {
		cfg := DefaultConfig()
		thumbData, size, err := Generate(testImageData, cfg)

		if err != nil {
			t.Fatalf("Generate() error = %v, want nil", err)
		}

		if size <= 0 {
			t.Errorf("Generate() size = %d, want > 0", size)
		}

		if int64(len(thumbData)) != size {
			t.Errorf("Generate() returned size %d but data length is %d", size, len(thumbData))
		}

		_, err = png.Decode(bytes.NewReader(thumbData))
		if err != nil {
			t.Errorf("Generated thumbnail is not a valid PNG: %v", err)
		}
	})

	t.Run("InvalidImageData", func(t *testing.T) {
		cfg := DefaultConfig()
		invalidData := []byte{0x00, 0x01, 0x02, 0x03}

		_, _, err := Generate(invalidData, cfg)
		if err == nil {
			t.Error("Generate() with invalid data should return error")
		}
	})

	t.Run("EmptyData", func(t *testing.T) {
		cfg := DefaultConfig()
		emptyData := []byte{}

		_, _, err := Generate(emptyData, cfg)
		if err == nil {
			t.Error("Generate() with empty data should return error")
		}
	})

	t.Run("NilConfig", func(t *testing.T) {
		thumbData, size, err := Generate(testImageData, nil)

		if err != nil {
			t.Fatalf("Generate() with nil config error = %v, want nil", err)
		}

		if size <= 0 {
			t.Errorf("Generate() size = %d, want > 0", size)
		}

		if len(thumbData) == 0 {
			t.Error("Generate() with nil config should still generate thumbnail")
		}
	})
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxWidth != 300 {
		t.Errorf("DefaultConfig().MaxWidth = %d, want 300", cfg.MaxWidth)
	}

	if cfg.MaxHeight != 300 {
		t.Errorf("DefaultConfig().MaxHeight = %d, want 300", cfg.MaxHeight)
	}

	if cfg.Quality != 100 {
		t.Errorf("DefaultConfig().Quality = %d, want 100", cfg.Quality)
	}

	if cfg.Format != "png" {
		t.Errorf("DefaultConfig().Format = %q, want \"png\"", cfg.Format)
	}
}
