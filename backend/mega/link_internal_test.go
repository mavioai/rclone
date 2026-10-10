package mega

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	mega "github.com/t3rm1n4l/go-mega"
)

func TestChunkRefused(t *testing.T) {
	for _, test := range []struct {
		err  string
		want bool
	}{
		{"Http Status: 403 Forbidden", true},
		{"Http Status: 404 Not Found", true},
		{"Http Status: 410 Gone", true},
		{"Http Status: 509 Bandwidth Limit Exceeded", false},
		{"Http Status: 500 Internal Server Error", false},
		{"wrong size for downloaded chunk", false},
	} {
		got := chunkRefused(errors.New(test.err))
		assert.Equal(t, test.want, got, test.err)
	}
}

func TestDownloadFinishesOnceComplete(t *testing.T) {
	// A download of no chunks is complete; its MAC is checked once
	d := &download{d: &mega.Download{}}
	assert.NoError(t, d.finish())
	assert.True(t, d.checked)

	// One with chunks left is not checked
	d = &download{d: &mega.Download{}, done: make([]bool, 2), left: 2}
	assert.NoError(t, d.finish())
	assert.False(t, d.checked)
}
