package readers

import (
	"io"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkipToRange(t *testing.T) {
	const content = "0123456789"
	for _, test := range []struct {
		name    string
		size    int64
		options []fs.OpenOption
		want    string
		wantErr bool
	}{
		{"none", 10, nil, content, false},
		{"seek", 10, []fs.OpenOption{&fs.SeekOption{Offset: 3}}, "3456789", false},
		{"range", 10, []fs.OpenOption{&fs.RangeOption{Start: 2, End: 4}}, "234", false},
		{"open range", 10, []fs.OpenOption{&fs.RangeOption{Start: 7, End: -1}}, "789", false},
		{"suffix", 10, []fs.OpenOption{&fs.RangeOption{Start: -1, End: 2}}, "89", false},
		{"suffix of unknown size", -1, []fs.OpenOption{&fs.RangeOption{Start: -1, End: 2}}, "", true},
		{"beyond the end", 10, []fs.OpenOption{&fs.SeekOption{Offset: 20}}, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rc, err := SkipToRange(io.NopCloser(strings.NewReader(content)), test.size, test.options)
			if test.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			assert.Equal(t, test.want, string(got))
		})
	}
}

func TestRangeRequested(t *testing.T) {
	assert.False(t, RangeRequested(nil))
	assert.False(t, RangeRequested([]fs.OpenOption{&fs.SeekOption{Offset: 0}}))
	assert.False(t, RangeRequested([]fs.OpenOption{&fs.RangeOption{Start: 0, End: -1}}))
	assert.True(t, RangeRequested([]fs.OpenOption{&fs.SeekOption{Offset: 1}}))
	assert.True(t, RangeRequested([]fs.OpenOption{&fs.RangeOption{Start: 0, End: 9}}))
	assert.True(t, RangeRequested([]fs.OpenOption{&fs.RangeOption{Start: -1, End: 9}}))
}
