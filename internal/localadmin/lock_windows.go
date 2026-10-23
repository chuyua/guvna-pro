//go:build windows

package localadmin

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockSocketFile(file *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
}

// AF_UNIX entries do not offer portable POSIX ownership metadata on Windows.
// Keep existing paths intact; a private parent ACL still protects new sockets.
func ownedByCurrentUser(os.FileInfo) bool { return false }
