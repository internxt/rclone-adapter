package thumbnails

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"strings"
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

// testJPEG returns a w x h JPEG whose left half is black and right half white.
// It is encoded from colour so it decodes to an *image.YCbCr.
func testJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.Draw(img, img.Rect, image.Black, image.Point{}, draw.Src)
	draw.Draw(img, image.Rect(w/2, 0, w, h), image.White, image.Point{}, draw.Src)
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 95}); err != nil {
		t.Fatalf("failed to encode test image: %v", err)
	}
	return buf.Bytes()
}

// writeChunks writes data to w in small chunks, like a stream would.
func writeChunks(t *testing.T, w io.Writer, data []byte) {
	t.Helper()
	for len(data) > 0 {
		n := min(len(data), 1000)
		if _, err := w.Write(data[:n]); err != nil {
			t.Fatalf("Write() error = %v, want nil", err)
		}
		data = data[n:]
	}
}

func TestGenerator(t *testing.T) {
	t.Run("StreamedImage", func(t *testing.T) {
		g := NewGenerator(nil)
		writeChunks(t, g, testJPEG(t, 1200, 600))
		thumb, err := g.Finish(nil)
		if err != nil {
			t.Fatalf("Finish() error = %v, want nil", err)
		}
		img, err := png.Decode(bytes.NewReader(thumb))
		if err != nil {
			t.Fatalf("thumbnail is not a valid PNG: %v", err)
		}
		if got := img.Bounds().Size(); got != image.Pt(300, 150) {
			t.Errorf("thumbnail size = %v, want (300,150)", got)
		}
	})

	t.Run("TrailingData", func(t *testing.T) {
		g := NewGenerator(nil)
		writeChunks(t, g, testJPEG(t, 600, 600))
		writeChunks(t, g, make([]byte, 100000))
		if _, err := g.Finish(nil); err != nil {
			t.Fatalf("Finish() error = %v, want nil", err)
		}
	})

	t.Run("InvalidImageData", func(t *testing.T) {
		g := NewGenerator(nil)
		writeChunks(t, g, bytes.Repeat([]byte{0x00, 0x01, 0x02, 0x03}, 100000))
		if _, err := g.Finish(nil); err == nil {
			t.Error("Finish() with invalid data should return error")
		}
	})

	t.Run("IncompleteData", func(t *testing.T) {
		g := NewGenerator(nil)
		data := testJPEG(t, 600, 600)
		writeChunks(t, g, data[:len(data)/2])
		uploadErr := errors.New("upload failed")
		if _, err := g.Finish(uploadErr); err != uploadErr {
			t.Errorf("Finish() error = %v, want %v", err, uploadErr)
		}
	})
}

func TestGenerateTooLarge(t *testing.T) {
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewGray(image.Rect(0, 0, 1, 1))); err != nil {
		t.Fatalf("failed to encode test image: %v", err)
	}
	data := buf.Bytes()
	// Rewrite the IHDR chunk (data[8:33]) to claim 20000x20000 pixels.
	binary.BigEndian.PutUint32(data[16:], 20000)
	binary.BigEndian.PutUint32(data[20:], 20000)
	binary.BigEndian.PutUint32(data[29:], crc32.ChecksumIEEE(data[12:29]))

	_, _, err := Generate(data, nil)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Errorf("Generate() error = %v, want too large error", err)
	}
}

func TestShrink(t *testing.T) {
	ratios := []image.YCbCrSubsampleRatio{
		image.YCbCrSubsampleRatio444,
		image.YCbCrSubsampleRatio422,
		image.YCbCrSubsampleRatio420,
		image.YCbCrSubsampleRatio440,
		image.YCbCrSubsampleRatio411,
		image.YCbCrSubsampleRatio410,
	}
	for _, ratio := range ratios {
		t.Run(ratio.String(), func(t *testing.T) {
			// Offset bounds exercise the plane offsets of a sub-image.
			src := image.NewYCbCr(image.Rect(-7, 3, 1001, 603), ratio)
			want := color.RGBA{0x20, 0x80, 0xc0, 0xff}
			yy, cb, cr := color.RGBToYCbCr(want.R, want.G, want.B)
			for i := range src.Y {
				src.Y[i] = yy
			}
			for i := range src.Cb {
				src.Cb[i], src.Cr[i] = cb, cr
			}
			sub := src.SubImage(image.Rect(1, 5, 999, 601)).(*image.YCbCr)

			dst := shrink(sub, 100, 50)
			if dst == nil {
				t.Fatal("shrink() = nil, want image")
			}
			if got := dst.Bounds(); got != image.Rect(0, 0, 100, 50) {
				t.Fatalf("shrink() bounds = %v, want (0,0)-(100,50)", got)
			}
			got := color.RGBAModel.Convert(dst.At(37, 21)).(color.RGBA)
			if absDiff(got.R, want.R) > 2 || absDiff(got.G, want.G) > 2 || absDiff(got.B, want.B) > 2 || got.A != 0xff {
				t.Errorf("shrink() pixel = %v, want %v", got, want)
			}
		})
	}

	t.Run("Averages", func(t *testing.T) {
		src := image.NewGray(image.Rect(0, 0, 400, 200))
		for y := range 200 {
			for x := range 400 {
				if x%2 == 0 {
					src.Pix[y*src.Stride+x] = 0xff
				}
			}
		}
		dst := shrink(src, 100, 50).(*image.Gray)
		for _, v := range dst.Pix {
			if v != 0x80 {
				t.Fatalf("shrink() pixel = %#x, want 0x80", v)
			}
		}
	})

	t.Run("ChromaSmallerThanDestination", func(t *testing.T) {
		src := image.NewYCbCr(image.Rect(0, 0, 400, 300), image.YCbCrSubsampleRatio420)
		if dst := shrink(src, 300, 225); dst != nil {
			t.Error("shrink() should return nil when a plane is smaller than the destination")
		}
		if got := fit(src, 300, 300).Bounds(); got != image.Rect(0, 0, 300, 225) {
			t.Errorf("fit() bounds = %v, want (0,0)-(300,225)", got)
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		if dst := shrink(image.NewNRGBA(image.Rect(0, 0, 400, 300)), 100, 75); dst != nil {
			t.Error("shrink() should return nil for an unsupported image type")
		}
	})
}

func absDiff(a, b uint8) uint8 {
	if a > b {
		return a - b
	}
	return b - a
}
