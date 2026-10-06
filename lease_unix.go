//go:build unix

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

func lockDatabase(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}
