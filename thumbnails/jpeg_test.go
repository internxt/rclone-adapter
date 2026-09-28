package thumbnails

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"
)

// exifJPEG returns a w x h JPEG with an EXIF orientation of o, stored with
// order, whose top left quadrant is white and the rest black.
func exifJPEG(t *testing.T, w, h, o int, order binary.ByteOrder) []byte {
	t.Helper()
	img := image.NewGray(image.Rect(0, 0, w, h))
	for y := range h / 2 {
		for x := range w / 2 {
			img.Pix[y*img.Stride+x] = 0xff
		}
	}
	var enc bytes.Buffer
	if err := jpeg.Encode(&enc, img, &jpeg.Options{Quality: 95}); err != nil {
		t.Fatal(err)
	}

	tiff := make([]byte, 26)
	if order == binary.LittleEndian {
		copy(tiff, "II")
	} else {
		copy(tiff, "MM")
	}
	order.PutUint16(tiff[2:], 42)
	order.PutUint32(tiff[4:], 8)
	order.PutUint16(tiff[8:], 1)       // one IFD entry
	order.PutUint16(tiff[10:], 0x0112) // orientation
	order.PutUint16(tiff[12:], 3)      // SHORT
	order.PutUint32(tiff[14:], 1)
	order.PutUint16(tiff[18:], uint16(o))
	app1 := append([]byte("Exif\x00\x00"), tiff...)

	out := []byte{0xff, 0xd8, 0xff, 0xe1, 0, 0}
	binary.BigEndian.PutUint16(out[4:], uint16(len(app1)+2))
	out = append(out, app1...)
	return append(out, enc.Bytes()[2:]...)
}

// progressiveJPEG returns data, a baseline JPEG, relabelled as a progressive
// one of w x h pixels. Only its header is valid.
func progressiveJPEG(t *testing.T, data []byte, w, h int) []byte {
	t.Helper()
	data = bytes.Clone(data)
	i := bytes.Index(data, []byte{0xff, 0xc0})
	if i < 0 {
		t.Fatal("no baseline frame header")
	}
	data[i+1] = 0xc2
	binary.BigEndian.PutUint16(data[i+5:], uint16(h))
	binary.BigEndian.PutUint16(data[i+7:], uint16(w))
	return data
}

func TestJPEGHeader(t *testing.T) {
	for o := 1; o <= 8; o++ {
		for _, order := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
			got, progressive := jpegHeader(exifJPEG(t, 16, 16, o, order))
			if got != o || progressive {
				t.Errorf("jpegHeader() = %d, %v, want %d, false (%v)", got, progressive, o, order)
			}
		}
	}
	got, progressive := jpegHeader(progressiveJPEG(t, exifJPEG(t, 16, 16, 6, binary.BigEndian), 16, 16))
	if got != 6 || !progressive {
		t.Errorf("progressive: jpegHeader() = %d, %v, want 6, true", got, progressive)
	}
	for name, b := range map[string][]byte{
		"NoEXIF":    testJPEG(t, 16, 16),
		"NotJPEG":   []byte("not a jpeg"),
		"Truncated": exifJPEG(t, 16, 16, 6, binary.BigEndian)[:20],
	} {
		if got, progressive := jpegHeader(b); got != 1 || progressive {
			t.Errorf("%s: jpegHeader() = %d, %v, want 1, false", name, got, progressive)
		}
	}
}

func TestGenerateProgressiveTooLarge(t *testing.T) {
	// Only the header is read, so the image data needn't match its size.
	_, _, err := Generate(progressiveJPEG(t, testJPEG(t, 16, 16), 4000, 3000), nil)
	if err == nil || !strings.Contains(err.Error(), "progressive JPEG too large") {
		t.Errorf("Generate() error = %v, want progressive JPEG too large error", err)
	}
}

func TestGenerateOrientation(t *testing.T) {
	// For each orientation, the size of the upright thumbnail of a 1200x600
	// image and which of its corners shows the white quadrant.
	tests := []struct {
		size  image.Point
		white image.Point
	}{
		1: {image.Pt(300, 150), image.Pt(0, 0)},
		2: {image.Pt(300, 150), image.Pt(1, 0)},
		3: {image.Pt(300, 150), image.Pt(1, 1)},
		4: {image.Pt(300, 150), image.Pt(0, 1)},
		5: {image.Pt(150, 300), image.Pt(0, 0)},
		6: {image.Pt(150, 300), image.Pt(1, 0)},
		7: {image.Pt(150, 300), image.Pt(1, 1)},
		8: {image.Pt(150, 300), image.Pt(0, 1)},
	}
	for o := 1; o <= 8; o++ {
		thumb, _, err := Generate(exifJPEG(t, 1200, 600, o, binary.BigEndian), nil)
		if err != nil {
			t.Fatalf("orientation %d: Generate() error = %v", o, err)
		}
		img, err := png.Decode(bytes.NewReader(thumb))
		if err != nil {
			t.Fatalf("orientation %d: %v", o, err)
		}
		size := img.Bounds().Size()
		if size != tests[o].size {
			t.Errorf("orientation %d: size = %v, want %v", o, size, tests[o].size)
			continue
		}
		for _, corner := range []image.Point{{0, 0}, {1, 0}, {0, 1}, {1, 1}} {
			x := 10 + corner.X*(size.X-21)
			y := 10 + corner.Y*(size.Y-21)
			gray := color.GrayModel.Convert(img.At(x, y)).(color.Gray).Y
			if white := gray > 0x80; white != (corner == tests[o].white) {
				t.Errorf("orientation %d: corner %v gray = %#x, want white only at %v", o, corner, gray, tests[o].white)
			}
		}
	}
}

func TestOrient(t *testing.T) {
	const w, h = 3, 2
	// Offset bounds check the transforms allow for the image origin.
	src := image.NewNRGBA(image.Rect(5, 7, 5+w, 7+h))
	// Orientation only applies to JPEGs, which are opaque.
	for i := range src.Pix {
		src.Pix[i] = uint8(i)
		if i%4 == 3 {
			src.Pix[i] = 0xff
		}
	}
	// stored returns the stored pixel displayed at (dx, dy).
	stored := func(o, dx, dy int) (sx, sy int) {
		switch o {
		case 1:
			return dx, dy
		case 2:
			return w - 1 - dx, dy
		case 3:
			return w - 1 - dx, h - 1 - dy
		case 4:
			return dx, h - 1 - dy
		case 5:
			return dy, dx
		case 6:
			return dy, h - 1 - dx
		case 7:
			return w - 1 - dy, h - 1 - dx
		default:
			return w - 1 - dy, dx
		}
	}
	for o := 1; o <= 8; o++ {
		dst := orient(src, o)
		size := image.Pt(w, h)
		if o >= 5 {
			size = image.Pt(h, w)
		}
		if got := dst.Bounds().Size(); got != size {
			t.Errorf("orientation %d: size = %v, want %v", o, got, size)
			continue
		}
		for dy := range size.Y {
			for dx := range size.X {
				sx, sy := stored(o, dx, dy)
				got := dst.At(dst.Bounds().Min.X+dx, dst.Bounds().Min.Y+dy)
				want := src.At(src.Rect.Min.X+sx, src.Rect.Min.Y+sy)
				if got != want {
					t.Errorf("orientation %d: pixel (%d,%d) = %v, want %v", o, dx, dy, got, want)
				}
			}
		}
	}
}
