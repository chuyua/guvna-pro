// Package localadmin provides filesystem-socket-only management transport.
package localadmin

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

func DefaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".guvna"
	}
	return filepath.Join(home, ".guvna")
}

func SocketPath(dataDir string) string {
	if path := os.Getenv("GUVNA_ADMIN_SOCKET"); path != "" {
		return path
	}
	return filepath.Join(dataDir, "admin.sock")
}

func filesystemPath(path string) (string, error) {
	if path == "" || strings.HasPrefix(path, "@") || strings.ContainsRune(path, 0) {
		return "", fmt.Errorf("management requires a filesystem socket path")
	}
	return filepath.Abs(path)
}

// Listen recovers an abandoned socket after an unclean shutdown. A path lock
// covers inspection, removal and binding so concurrent starts cannot unlink a
// new listener. User files, symlinks and sockets that may be live are preserved.
func Listen(path string) (net.Listener, error) {
	path, err := filesystemPath(path)
	if err != nil {
		return nil, err
	}
	lock, err := socketPathLock(path)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := clearStaleSocket(path); err != nil {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("management socket %s: %w", path, err)
	}
	// Windows AF_UNIX objects do not implement POSIX chmod; their access uses
	// the containing directory's ACL. Bearer authentication is still required.
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o600); err != nil {
			listener.Close()
			return nil, fmt.Errorf("management socket permissions: %w", err)
		}
	}
	return listener, nil
}

// The lock file is kept after unlocking. Removing it would allow a new opener
// to lock a different inode while another process still holds the old file.
func socketPathLock(path string) (*os.File, error) {
	lockPath := path + ".lock"
	if info, err := os.Lstat(lockPath); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("management socket lock is not a regular file: %s", lockPath)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("management socket lock: %w", err)
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("management socket lock: %w", err)
	}
	info, statErr := lock.Stat()
	current, pathErr := os.Lstat(lockPath)
	if statErr != nil || pathErr != nil || !info.Mode().IsRegular() || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		lock.Close()
		return nil, fmt.Errorf("management socket lock path changed: %s", lockPath)
	}
	if err := lockSocketFile(lock); err != nil {
		lock.Close()
		return nil, fmt.Errorf("management socket initialization is busy or unavailable: %w", err)
	}
	return lock, nil
}

// clearStaleSocket requires the path lock. Only an owned socket that explicitly
// refuses connections can be removed; timeout, permission and unknown failures
// leave the path intact. The private parent directory is part of this contract.
func clearStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("management socket: %w", err)
	}
	exists := fmt.Errorf("management socket path already exists: %s (check for a running instance)", path)
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || !ownedByCurrentUser(info) {
		return exists
	}
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		conn.Close()
		return exists
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return exists
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, current) {
		return exists
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("management socket: remove stale %s: %w", path, err)
	}
	log.Printf("management socket: removed abandoned socket %s", path)
	return nil
}

// Client never consults proxy settings, dials TCP, or follows a redirect.
func Client(path string, timeout time.Duration) *http.Client {
	dialer := &net.Dialer{}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				path, err := filesystemPath(path)
				if err != nil {
					return nil, err
				}
				return dialer.DialContext(ctx, "unix", path)
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
