//go:build !windows

package tools

import (
	"os/exec"
	"syscall"
)

// setupProcessGroup makes the child process the leader of its own process
// group. This lets us kill the entire tree (bash + any grandchildren it
// spawned) with a single negative-pid signal instead of leaving orphans
// behind when a command times out or is killed.
func setupProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killProcessTree sends SIGKILL to the whole process group of the given
// process. On platforms where the child was started with Setpgid this covers
// bash itself plus every descendant. Returns nil when there is no process
// (e.g. it already exited).
func killProcessTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	// A negative pid targets the process group whose leader is cmd.Process.Pid.
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
