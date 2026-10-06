//go:build !unix

package main

import (
	"fmt"
	"os"
)

func lockDatabase(*os.File) error {
	return fmt.Errorf("SQLite singleton locking is unsupported on this platform; use PostgreSQL")
}
