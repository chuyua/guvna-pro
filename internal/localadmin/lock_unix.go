//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package localadmin

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func lockSocketFile(file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err != unix.EINTR {
			return err
		}
	}
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid()
}
