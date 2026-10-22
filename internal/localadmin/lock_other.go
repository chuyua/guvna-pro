//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package localadmin

import (
	"fmt"
	"os"
)

func lockSocketFile(*os.File) error {
	return fmt.Errorf("management socket locking is unsupported on this platform")
}

func ownedByCurrentUser(os.FileInfo) bool { return false }
