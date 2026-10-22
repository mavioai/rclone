package fs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOfflineTasksBefore(t *testing.T) {
	since := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	before, after := since.Add(-time.Hour), since.Add(time.Hour)
	page := func(added ...time.Time) []*OfflineTask {
		var p []*OfflineTask
		for _, a := range added {
			p = append(p, &OfflineTask{CreatedAt: a})
		}
		return p
	}
	for _, test := range []struct {
		name  string
		page  []*OfflineTask
		since time.Time
		want  bool
	}{
		{"all before", page(before, before), since, true},
		{"one after", page(after, before), since, false},
		{"unknown time", page(before, time.Time{}), since, false},
		{"empty page", nil, since, false},
		{"zero since", page(before), time.Time{}, false},
	} {
		assert.Equal(t, test.want, OfflineTasksBefore(test.page, test.since), test.name)
	}
}
