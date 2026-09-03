package buckets

import "testing"

func TestSplitNameExt(t *testing.T) {
	testCases := []struct {
		fileName  string
		plainName string
		fileType  string
		display   string
	}{
		{"foo.txt", "foo", "txt", "foo.txt"},
		{"foo.tar.gz", "foo.tar", "gz", "foo.tar.gz"},
		{"README", "README", "", "README"},
		{"a.b.c.d", "a.b.c", "d", "a.b.c.d"},
		// A leading dot is part of the name, not an extension separator.
		{".bashrc", ".bashrc", "", ".bashrc"},
		{".env", ".env", "", ".env"},
		{".gitignore", ".gitignore", "", ".gitignore"},
		// Only the first dot is special: a later one still splits.
		{"..hidden", ".", "hidden", "..hidden"},
		{".a.b", ".a", "b", ".a.b"},
		{"a..b", "a.", "b", "a..b"},
		// A trailing dot is dropped, as it is by every other client.
		{"foo.", "foo", "", "foo"},
		{"....", "...", "", "..."},
		{".", ".", "", "."},
		// Only the last path element is stored.
		{"dir/foo.txt", "foo", "txt", "foo.txt"},
	}
	for _, tc := range testCases {
		t.Run(tc.fileName, func(t *testing.T) {
			plainName, fileType := SplitNameExt(tc.fileName)
			if plainName != tc.plainName || fileType != tc.fileType {
				t.Errorf("SplitNameExt(%q) = (%q, %q), want (%q, %q)",
					tc.fileName, plainName, fileType, tc.plainName, tc.fileType)
			}
			if display := JoinNameExt(plainName, fileType); display != tc.display {
				t.Errorf("JoinNameExt(%q, %q) = %q, want %q",
					plainName, fileType, display, tc.display)
			}
		})
	}
}
