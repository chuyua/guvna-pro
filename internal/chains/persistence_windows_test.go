package chains

import (
	"bytes"
	"os"
	"syscall"
	"testing"
)

func TestSaveLockedTargetPreservesOriginal(t *testing.T) {
	s := New(t.TempDir())
	original := []byte("chains: []\n")
	if err := os.WriteFile(s.path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	path, err := syscall.UTF16PtrFromString(s.path)
	if err != nil {
		t.Fatal(err)
	}
	// Allow reads but deny deletion/replacement, as another Windows process can.
	handle, err := syscall.CreateFile(path, syscall.GENERIC_READ, syscall.FILE_SHARE_READ, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.CloseHandle(handle) })
	if err := s.Save(newRouter()); err == nil {
		t.Fatal("expected replacement of a locked target to fail")
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, original) {
		t.Fatalf("failed save changed the original file: %q", data)
	}
	assertNoTempFiles(t, s)
}
