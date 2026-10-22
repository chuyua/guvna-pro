//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package localadmin

import (
	"os"
	"syscall"
	"testing"
)

type ownerFileInfo struct {
	os.FileInfo
	stat syscall.Stat_t
}

func (info ownerFileInfo) Sys() any { return &info.stat }

func TestSocketOwnerMustMatchEffectiveUser(t *testing.T) {
	if !ownedByCurrentUser(ownerFileInfo{stat: syscall.Stat_t{Uid: uint32(os.Geteuid())}}) {
		t.Fatal("current user's socket was not recognized")
	}
	if ownedByCurrentUser(ownerFileInfo{stat: syscall.Stat_t{Uid: uint32(os.Geteuid() + 1)}}) {
		t.Fatal("another user's socket was accepted for cleanup")
	}
}
