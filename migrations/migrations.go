// Package migrations embeds the SQL migration files so they ship inside the
// single binary (no filesystem dependency at deploy time). The store layer
// feeds FS to golang-migrate's iofs source driver.
package migrations

import "embed"

// FS holds every *.sql migration in this directory, rooted at the FS root.
//
//go:embed *.sql
var FS embed.FS
