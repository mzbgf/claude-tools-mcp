//go:build windows

package tools

import "os/exec"

// setupProcessGroup is a no-op on Windows: there is no POSIX process-group
// kill, so we fall back to killing just the direct child.
func setupProcessGroup(cmd *exec.Cmd) {
	// no-op
}

// killProcessTree falls back to killing only the direct child process.
func killProcessTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
