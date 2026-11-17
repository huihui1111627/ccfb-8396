// Package web embeds the management console single-page UI so the server
// ships as a single binary with no external asset directory.
package web

import "embed"

//go:embed index.html
var assets embed.FS

// FS returns the embedded filesystem rooted at the package directory.
func FS() embed.FS { return assets }
