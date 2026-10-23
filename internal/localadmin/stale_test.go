package localadmin

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func abandonSocket(t *testing.T, path string) {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket fixture is missing: %v", err)
	}
}

func TestListenRecoversStaleSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows preserves existing AF_UNIX paths without portable owner metadata")
	}
	path := shortSocket(t)
	abandonSocket(t, path)
	listener, err := Listen(path)
	if err != nil {
		t.Fatalf("restart after unclean shutdown failed: %v", err)
	}
	defer listener.Close()
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("recovered socket is unavailable: %v", err)
	}
	conn.Close()
}

type socketContender struct {
	command *exec.Cmd
	input   io.WriteCloser
	lines   <-chan string
	stderr  bytes.Buffer
}

func startSocketContender(t *testing.T, path string) *socketContender {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	contender := &socketContender{command: exec.Command(executable, "-test.run=^TestSocketStartupHelper$")}
	contender.command.Env = append(os.Environ(), "GUVNA_SOCKET_STARTUP_HELPER=1", "GUVNA_SOCKET_STARTUP_PATH="+path)
	contender.command.Stderr = &contender.stderr
	contender.input, err = contender.command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := contender.command.StdoutPipe()
	if err != nil {
		contender.input.Close()
		t.Fatal(err)
	}
	if err := contender.command.Start(); err != nil {
		contender.input.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		contender.input.Close()
		if contender.command.ProcessState == nil {
			_ = contender.command.Process.Kill()
			_ = contender.command.Wait()
		}
	})
	lines := make(chan string, 8)
	contender.lines = lines
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	return contender
}

func contenderLine(t *testing.T, contender *socketContender) string {
	t.Helper()
	select {
	case line, ok := <-contender.lines:
		if !ok {
			t.Fatal("socket contender exited before reporting a result")
		}
		return line
	case <-time.After(10 * time.Second):
		t.Fatal("socket contender did not report a result")
		return ""
	}
}

func TestSocketStartupHonorsProcessLock(t *testing.T) {
	path := shortSocket(t)
	lock, err := socketPathLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	contender := startSocketContender(t, path)
	if line := contenderLine(t, contender); line != "ready" {
		t.Fatalf("socket contender was not ready: %q", line)
	}
	if _, err := io.WriteString(contender.input, "start\n"); err != nil {
		t.Fatal(err)
	}
	if line := contenderLine(t, contender); line != "refused" {
		t.Fatalf("startup bypassed another process's path lock: %q", line)
	}
	contender.input.Close()
	if err := contender.command.Wait(); err != nil {
		t.Fatalf("socket contender failed: %v: %s", err, contender.stderr.String())
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("locked startup created a socket: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := Listen(path)
	if err != nil {
		t.Fatalf("released path lock remained stuck: %v", err)
	}
	listener.Close()
}

func TestConcurrentSocketStartKeepsWinner(t *testing.T) {
	path := shortSocket(t)
	if runtime.GOOS != "windows" {
		abandonSocket(t, path)
	}
	contenders := make([]*socketContender, 4)
	for i := range contenders {
		contenders[i] = startSocketContender(t, path)
		if line := contenderLine(t, contenders[i]); line != "ready" {
			t.Fatalf("socket contender was not ready: %q", line)
		}
	}
	for _, contender := range contenders {
		if _, err := io.WriteString(contender.input, "start\n"); err != nil {
			t.Fatal(err)
		}
	}
	winners := 0
	for _, contender := range contenders {
		switch line := contenderLine(t, contender); line {
		case "listening":
			winners++
		case "refused":
		default:
			t.Fatalf("unexpected socket contender result: %q", line)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent starts created %d listeners, want one", winners)
	}
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("a losing startup removed the winning socket: %v", err)
	}
	conn.Close()
	for _, contender := range contenders {
		contender.input.Close()
		if err := contender.command.Wait(); err != nil {
			t.Fatalf("socket contender failed: %v: %s", err, contender.stderr.String())
		}
	}
}

func TestSocketStartupHelper(t *testing.T) {
	if os.Getenv("GUVNA_SOCKET_STARTUP_HELPER") != "1" {
		return
	}
	input := bufio.NewReader(os.Stdin)
	fmt.Fprintln(os.Stdout, "ready")
	if _, err := input.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	listener, err := Listen(os.Getenv("GUVNA_SOCKET_STARTUP_PATH"))
	if err != nil {
		fmt.Fprintln(os.Stdout, "refused")
		return
	}
	defer listener.Close()
	fmt.Fprintln(os.Stdout, "listening")
	_, _ = input.ReadString('\n')
}

func TestListenRefusesSocketLockSymlink(t *testing.T) {
	path := shortSocket(t)
	target := path + ".target"
	if err := os.WriteFile(target, []byte("preserve me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path+".lock"); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("socket initialization followed a lock symlink")
	}
	if body, err := os.ReadFile(target); err != nil || string(body) != "preserve me" {
		t.Fatalf("lock symlink target changed: %q %v", body, err)
	}
	if info, err := os.Lstat(path + ".lock"); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("lock symlink changed: %v", err)
	}
}
