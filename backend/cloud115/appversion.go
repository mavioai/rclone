package cloud115

import (
	"context"
	"sync"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/lib/rest"
)

const (
	// defaultAppVersion is used when 115 doesn't tell the current
	// version of its browser
	defaultAppVersion = "36.0.1"
	// appVersionTimeout limits asking 115 for the version
	appVersionTimeout = 10 * time.Second
)

var (
	appVersionOnce sync.Once
	appVersion     string
)

// versionResponse is 115's answer listing its apps' current versions
type versionResponse struct {
	Data struct {
		Win struct {
			Version string `json:"version_code"`
		} `json:"win"`
	} `json:"data"`
}

// currentAppVersion returns the current version of 115's Windows
// browser, asked of 115 once, as OpenList does. The API's firewall
// blocks clients claiming an outdated browser, and uploads and offline
// tasks state the version too.
func currentAppVersion(ctx context.Context) string {
	appVersionOnce.Do(func() {
		appVersion = defaultAppVersion
		ctx, cancel := context.WithTimeout(ctx, appVersionTimeout)
		defer cancel()
		var resp versionResponse
		_, err := rest.NewClient(fshttp.NewClient(ctx)).CallJSON(ctx, &rest.Opts{
			Method:  "GET",
			RootURL: driver115.ApiGetVersion,
		}, nil, &resp)
		if err != nil {
			fs.Debugf(nil, "cloud115: get the app version, using %s: %v", appVersion, err)
			return
		}
		if v := resp.Data.Win.Version; v != "" {
			appVersion = v
		}
		fs.Debugf(nil, "cloud115: app version %s", appVersion)
	})
	return appVersion
}

// userAgentOf returns the user agent of 115's browser of version
func userAgentOf(version string) string {
	return "Mozilla/5.0 115Browser/" + version
}
