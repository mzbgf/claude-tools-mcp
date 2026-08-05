package tools

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// defaultTimeout is 2 minutes - chosen to prevent long-running operations
	// from blocking foreground execution while still allowing reasonable time
	// for most shell operations (git, npm, docker, etc.)
	defaultTimeout = 120000
	// maxTimeout is 10 minutes - enforced to prevent indefinite blocking
	// and memory exhaustion from extremely long-running processes
	maxTimeout = 600000
)

// BackgroundShell represents a long-running command executing asynchronously.
// LastStdoutReadAt and LastStderrReadAt track byte positions to support
// fetching only new output on subsequent reads, avoiding re-transmission
// of already-returned data and respecting size constraints.
type BackgroundShell struct {
	ID               string
	Command          string
	Description      string
	Cmd              *exec.Cmd
	Stdout           *SyncBuffer
	Stderr           *SyncBuffer
	StartTime        time.Time
	Done             chan struct{}
	Err              error
	ExitCode         int
	LastStdoutReadAt int
	LastStderrReadAt int
}

// bashExecResult carries the layered outcome of a foreground command so the
// MCP tool can return stdout / stderr / exit code separately to the agent.
// stderr has already been stripped of the wrapper shell's own artifacts
// (leading job-control warnings, trailing "logout").
type bashExecResult struct {
	stdout   string
	stderr   string // filtered
	exitCode int
	merged   string // stdout + stderr, the backward-compatible combined text
}

func (s *State) executeBashCommand(ctx context.Context, command, description string, timeout int64, runInBackground bool) (*bashExecResult, error) {
	if command == "" {
		return nil, fmt.Errorf("Command cannot be empty.")
	}

	timeoutMs := defaultTimeout
	if timeout > 0 {
		if timeout > maxTimeout {
			return nil, fmt.Errorf("Timeout cannot exceed %d milliseconds (10 minutes).", maxTimeout)
		}
		timeoutMs = int(timeout)
	}

	// Background commands don't use context timeout because they run asynchronously
	// and their output is retrieved later via BashOutput. Foreground commands use
	// context timeout to enforce synchronous execution limits.
	//
	// We spawn `bash -lic` instead of plain `bash -c`: -l (login) reads
	// /etc/profile + ~/.profile, -i (interactive) reads /etc/bash.bashrc +
	// ~/.bashrc, so the child gets the user's FULL shell environment (PATH
	// extensions, aliases, functions, nvm/conda/venv hooks, etc.) instead of
	// the bare inherited env. `-c` keeps one-shot semantics with a proper exit
	// code. Without a PTY this prints harmless job-control warnings to stderr
	// which we filter in executeForeground. Stdin is left nil (Go's exec treats
	// nil Stdin as /dev/null) so the interactive shell never reads from our
	// JSON-RPC pipe.
	var cmd *exec.Cmd
	if runInBackground {
		cmd = exec.Command("bash", "-lic", command)
	} else {
		cmdCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
		cmd = exec.CommandContext(cmdCtx, "bash", "-lic", command)
		// On timeout, kill the whole process group (bash + any grandchildren),
		// not just bash itself. Requires Setpgid (see proc_unix.go).
		cmd.Cancel = func() error { return killProcessTree(cmd) }
	}
	setupProcessGroup(cmd)

	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}

	if runInBackground {
		msg, err := s.executeBackground(cmd, command, description)
		if err != nil {
			return nil, err
		}
		return &bashExecResult{merged: msg}, nil
	}
	return s.executeForeground(ctx, cmd, command)
}

// ioGracePeriod is how long we keep draining stdout/stderr after the shell
// has exited. A foreground command that spawned a background process without
// redirecting its output keeps the pipes open (the grandchild inherited the
// write ends), so waiting for EOF would block forever. This mirrors pi's
// waitForChildProcess: after exit, give the pipes a short grace period and
// return whatever output has arrived.
const ioGracePeriod = 250 * time.Millisecond

func (s *State) executeForeground(ctx context.Context, cmd *exec.Cmd, command string) (*bashExecResult, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("Failed to create stdout pipe: %s", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("Failed to create stderr pipe: %s", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("Failed to start command: %s", err)
	}

	var outBuf, errBuf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { _, _ = io.Copy(&outBuf, stdout); wg.Done() }()
	go func() { _, _ = io.Copy(&errBuf, stderr); wg.Done() }()

	waitErr := cmd.Wait()

	// The shell has exited. Drain the pipes but bound the wait: background
	// grandchildren that inherited the pipe write ends would otherwise keep
	// us blocked forever (and we deliberately did NOT kill the process group
	// here - a normally-finished command must not reap its background jobs).
	ioDone := make(chan struct{})
	go func() { wg.Wait(); close(ioDone) }()
	select {
	case <-ioDone:
	case <-time.After(ioGracePeriod):
	}

	// The job-control warnings are printed by the wrapper bash to its stderr
	// at startup, and "logout" when the command explicitly exits the login
	// shell, so we filter the stderr stream and keep stdout untouched.
	stdoutText := outBuf.String()
	stderrText := filterJobControlNoise(errBuf.String())
	outcome := &bashExecResult{
		stdout:   stdoutText,
		stderr:   stderrText,
		exitCode: 0,
		merged:   stdoutText + stderrText,
	}
	if waitErr != nil {
		if strings.Contains(waitErr.Error(), "context deadline exceeded") {
			return nil, fmt.Errorf("Command timed out. Consider increasing the timeout parameter or running in background.")
		}

		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			exitCode := exitErr.ExitCode()
			// On Unix/Linux, a killed process (e.g., by timeout signal) returns exit code -1
			// rather than the actual signal number. Detect this to provide clearer error messaging.
			if exitCode == -1 && strings.Contains(waitErr.Error(), "signal: killed") {
				return nil, fmt.Errorf("Command timed out. Consider increasing the timeout parameter or running in background.")
			}
			outcome.exitCode = exitCode

			return nil, fmt.Errorf(
				"Command exited with code %d:\n%s\n\nCommand: %s",
				exitCode,
				outcome.merged,
				command,
			)
		}

		return nil, fmt.Errorf("Failed to execute command: %s\n\nCommand: %s", waitErr, command)
	}

	if err := checkOutputSize(ctx, outcome.merged, "bash"); err != nil {
		return nil, err
	}

	return outcome, nil
}

// filterJobControlNoise cleans up the stderr stream produced by the wrapper
// bash -lic, removing ONLY the wrapper's own artifacts and leaving the user
// command's output untouched:
//
//   - leading job-control warnings (bash prints them to stderr at startup
//     when there is no controlling terminal):
//     bash: cannot set terminal process group (NNNN): Inappropriate ioctl for device
//     bash: no job control in this shell
//     bash: [NNNN: 1 (255)] tcsetattr: Inappropriate ioctl for device
//   - a trailing "logout" line (a login shell prints it when the command
//     explicitly runs `exit`; at the very end of the stderr stream it can
//     only come from the wrapper shell itself - a nested `bash -lic` would
//     need to both exit itself AND leave its stderr unredirected to land
//     here, and manual `echo logout >&2` without an outer `exit` is equally
//     contrived, so this filter cannot realistically mis-strip real output).
//
// Anything in between - a nested `bash -lic` with redirected stderr, `echo
// logout` on stdout, real diagnostics - is preserved.
func filterJobControlNoise(output string) string {
	lines := strings.Split(output, "\n")

	// Leading wrapper warnings.
	i := 0
	for i < len(lines) && isJobControlNoise(lines[i]) {
		i++
	}

	// Trailing "logout" from an explicit exit of the wrapper login shell.
	// strings.Split yields a final "" for a trailing newline; skip it, then
	// drop a single exact "logout" line.
	j := len(lines)
	if j > i && lines[j-1] == "" {
		j--
	}
	if j > i && lines[j-1] == "logout" {
		j--
	}

	if i == 0 && j == len(lines) {
		return output
	}
	return strings.Join(lines[i:j], "\n")
}

func isJobControlNoise(line string) bool {
	return strings.HasPrefix(line, "bash: cannot set terminal process group") ||
		strings.HasPrefix(line, "bash: no job control in this shell") ||
		strings.Contains(line, "tcsetattr: Inappropriate ioctl for device")
}

func (s *State) executeBackground(cmd *exec.Cmd, command, description string) (string, error) {
	// SyncBuffer is needed because both the subprocess and the BashOutput
	// goroutine will read from stdout/stderr concurrently
	stdout := &SyncBuffer{}
	stderr := &SyncBuffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("Failed to start background command: %s", err)
	}

	s.Mu.Lock()
	shellID := fmt.Sprintf("shell_%d", s.NextShellID)
	s.NextShellID++
	shell := &BackgroundShell{
		ID:          shellID,
		Command:     command,
		Description: description,
		Cmd:         cmd,
		Stdout:      stdout,
		Stderr:      stderr,
		StartTime:   time.Now(),
		Done:        make(chan struct{}),
	}
	s.BackgroundShells[shellID] = shell
	s.Mu.Unlock()

	// Monitor process completion in a separate goroutine to avoid blocking
	// and to capture exit code/error for later retrieval
	go func() {
		err := cmd.Wait()
		s.Mu.Lock()
		defer s.Mu.Unlock()
		shell.Err = err
		if cmd.ProcessState != nil {
			shell.ExitCode = cmd.ProcessState.ExitCode()
		}
		close(shell.Done)
	}()

	return fmt.Sprintf("Command running in background with ID: %s", shellID), nil
}

// SyncBuffer wraps bytes.Buffer with a mutex to allow safe concurrent reads
// from both the subprocess (writing output) and the BashOutput handler
// (reading output). This is essential because the process writes continuously
// while callers may read asynchronously.
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (sb *SyncBuffer) Write(p []byte) (n int, err error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Write(p)
}

func (sb *SyncBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.String()
}

var (
	_ io.Writer = (*SyncBuffer)(nil)

	BashTool = sdk.Tool{
		Name:        "bash",
		Description: "Executes a given bash command in a persistent shell session with optional timeout, ensuring proper handling and security measures.\n\nIMPORTANT: This tool is for terminal operations like git, npm, docker, etc. DO NOT use it for file operations (reading, writing, editing, searching, finding files) - use the specialized tools for this instead.\n\nBefore executing the command, please follow these steps:\n\n1. Directory Verification:\n   - If the command will create new directories or files, first use `ls` to verify the parent directory exists and is the correct location\n   - For example, before running \"mkdir foo/bar\", first use `ls foo` to check that \"foo\" exists and is the intended parent directory\n\n2. Command Execution:\n   - Always quote file paths that contain spaces with double quotes (e.g., cd \"path with spaces/file.txt\")\n   - Examples of proper quoting:\n     - cd \"/Users/name/My Documents\" (correct)\n     - cd /Users/name/My Documents (incorrect - will fail)\n     - python \"/path/with spaces/script.py\" (correct)\n     - python /path/with spaces/script.py (incorrect - will fail)\n   - After ensuring proper quoting, execute the command.\n   - Capture the output of the command.\n\nUsage notes:\n  - The command argument is required.\n  - You can specify an optional timeout in milliseconds (up to 600000ms / 10 minutes). If not specified, commands will timeout after 120000ms (2 minutes).\n  - It is very helpful if you write a clear, concise description of what this command does in 5-10 words.\n  - You can use the `run_in_background` parameter to run the command in the background, which allows you to continue working while the command runs. You can monitor the output using the Bash tool as it becomes available. Never use `run_in_background` to run 'sleep' as it will return immediately. You do not need to use '&' at the end of the command when using this parameter.\n  \n  - Avoid using Bash with the `find`, `grep`, `cat`, `head`, `tail`, `sed`, `awk`, or `echo` commands, unless explicitly instructed or when these commands are truly necessary for the task. Instead, always prefer using the dedicated tools for these commands:\n    - File search: Use Glob (NOT find or ls)\n    - Content search: Use Grep (NOT grep or rg)\n    - Read files: Use Read (NOT cat/head/tail)\n    - Edit files: Use Edit (NOT sed/awk)\n    - Write files: Use Write (NOT echo >/cat <<EOF)\n    - Communication: Output text directly (NOT echo/printf)\n  - When issuing multiple commands:\n    - If the commands are independent and can run in parallel, make multiple Bash tool calls in a single message. For example, if you need to run \"git status\" and \"git diff\", send a single message with two Bash tool calls in parallel.\n    - If the commands depend on each other and must run sequentially, use a single Bash call with '&&' to chain them together (e.g., `git add . && git commit -m \"message\" && git push`). For instance, if one operation must complete before another starts (like mkdir before cp, Write before Bash for git operations, or git add before git commit), run these operations sequentially instead.\n    - Use ';' only when you need to run commands sequentially but don't care if earlier commands fail\n    - DO NOT use newlines to separate commands (newlines are ok in quoted strings)\n  - Try to maintain your current working directory throughout the session by using absolute paths and avoiding usage of `cd`. You may use `cd` if the User explicitly requests it.\n    <good-example>\n    pytest /foo/bar/tests\n    </good-example>\n    <bad-example>\n    cd /foo/bar && pytest tests\n    </bad-example>",
	}
)

type BashInput struct {
	Command         string `json:"command" jsonschema:"The command to execute"`
	Description     string `json:"description,omitempty" jsonschema:"Clear, concise description of what this command does in 5-10 words, in active voice. Examples:\nInput: ls\nOutput: List files in current directory\n\nInput: git status\nOutput: Show working tree status\n\nInput: npm install\nOutput: Install package dependencies\n\nInput: mkdir foo\nOutput: Create directory 'foo'"`
	RunInBackground bool   `json:"run_in_background,omitempty" jsonschema:"Set to true to run this command in the background. Use BashOutput to read the output later."`
	Timeout         int64  `json:"timeout,omitempty" jsonschema:"Optional timeout in milliseconds (max 600000)"`
}

// BashResult is the layered result returned in structuredContent. The text
// content (merged stdout+stderr) and this object are semantically equivalent
// presentations of the same information, per the MCP spec (SEP-1624): clients
// pick one - `content` for readability/token efficiency, `structuredContent`
// for programmatic access - and MUST NOT forward both to the model.
type BashResult struct {
	Stdout   string `json:"stdout,omitempty"` // raw stdout, never filtered
	Stderr   string `json:"stderr,omitempty"` // stderr with wrapper artifacts removed
	ExitCode int    `json:"exit_code"`        // exit code (0 on success)
}

func Bash(ctx context.Context, req *sdk.CallToolRequest, args BashInput) (*sdk.CallToolResult, any, error) {
	server := GetState()
	outcome, err := server.executeBashCommand(ctx, args.Command, args.Description, args.Timeout, args.RunInBackground)
	if err != nil {
		return nil, nil, err
	}

	output := &BashResult{
		Stdout:   outcome.stdout,
		Stderr:   outcome.stderr,
		ExitCode: outcome.exitCode,
	}
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: outcome.merged}},
		StructuredContent: output,
	}, output, nil
}
