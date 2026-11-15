// Package acpshim is protocol7's ACP shim: one detached process per agent
// session that starts the agent harness, owns its process group, serves ACP
// on a unix socket in the session's shim directory and journals every frame.
// The process is cmd/p7acpshim, built on the acphost library. Client is the
// other end, used by the session activities.
//
// The shim outlives the worker that started it. A worker that restarts
// connects again and reads what it missed from the journal.
package acpshim

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gfx-labs/acphost/pkg/acphost"
	"github.com/gfx-labs/acphost/pkg/proc"
)

const (
	// Binary is the name of the shim command.
	Binary = "p7acpshim"
	// methodPrefix names the shim's own methods on the socket, apart from ACP.
	methodPrefix = "_p7/"
	// gcRootPrefix names the Nix GC root a running shim holds for its binary.
	gcRootPrefix = "p7-shim-"
	// stopGrace bounds the wait for a shim to stop before it is killed.
	stopGrace = 30 * time.Second
)

// Spec says what a shim runs.
type Spec struct {
	// Session names the session. It must be one path element.
	Session string
	// Command and Args start the agent harness, which speaks ACP on its
	// standard input and output.
	Command string
	Args    []string
	// Env is added to the shim's own environment for the harness (KEY=VALUE).
	Env []string
	// WorkingDirectory is where the harness starts.
	WorkingDirectory string
	// Lease is how long the shim keeps the harness with no client connected.
	Lease time.Duration
}

// WriteSpec stores spec in the shim directory dir, creating it.
func WriteSpec(dir string, spec Spec) error {
	if spec.Lease <= 0 {
		return errors.New("shim spec: the lease must be positive, or the shim would never exit")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create shim directory: %w", err)
	}
	if err := acphost.CheckSocketPath(dir); err != nil {
		return err
	}
	return acphost.WriteSpec(dir, acphost.Spec{
		Protocol:     acphost.Protocol,
		Session:      spec.Session,
		Command:      spec.Command,
		Args:         spec.Args,
		Env:          spec.Env,
		Dir:          spec.WorkingDirectory,
		LeaseMS:      spec.Lease.Milliseconds(),
		MethodPrefix: methodPrefix,
		GCRootPrefix: gcRootPrefix,
	})
}

// JournalPath is the journal of the shim in dir.
func JournalPath(dir string) string { return filepath.Join(dir, acphost.JournalFile) }

// Alive reports whether a shim runs for dir.
func Alive(dir string) bool { return acphost.Alive(dir) }

// Find returns the path of the shim command: configured if it is set, else
// the one beside this executable, else the one in PATH.
func Find(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	if executable, err := os.Executable(); err == nil {
		beside := filepath.Join(filepath.Dir(executable), Binary)
		if info, err := os.Stat(beside); err == nil && !info.IsDir() {
			return beside, nil
		}
	}
	found, err := exec.LookPath(Binary)
	if err != nil {
		return "", errors.New(Binary + " was not found beside this executable or in PATH: build cmd/p7acpshim or name it with --shim")
	}
	return found, nil
}

// Start runs the shim command binary for dir, whose spec is written, and
// returns once the harness has started. The shim keeps running after binary
// exits. Starting a directory whose shim already runs is an error.
func Start(ctx context.Context, binary, dir string) error {
	command := exec.CommandContext(ctx, binary, "start", "--dir", dir)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("start shim in %s: %w: %s", dir, err, strings.TrimSpace(string(output)))
	}
	return nil
}

// Stop stops the shim of dir, if one runs, and waits for it to exit. The
// harness gets SIGTERM, and SIGKILL after the shim's grace period.
func Stop(ctx context.Context, dir string) error {
	if !acphost.Alive(dir) {
		return nil
	}
	detached, err := proc.Open(dir)
	if err != nil {
		return fmt.Errorf("find shim in %s: %w", dir, err)
	}
	if err := detached.Stop(); err != nil {
		return fmt.Errorf("stop shim in %s: %w", dir, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, stopGrace)
	defer cancel()
	if _, err := detached.Wait(waitCtx); err != nil {
		return fmt.Errorf("wait for shim in %s: %w", dir, err)
	}
	return nil
}

// ExitReason is why the shim of dir last stopped, or "" if that is not known.
func ExitReason(dir string) string {
	exit, ok := acphost.ReadExit(dir)
	if !ok {
		return ""
	}
	switch {
	case exit.Error != "":
		return exit.Reason + ": " + exit.Error
	case exit.Signal != "":
		return exit.Reason + " (harness killed by " + exit.Signal + ")"
	default:
		return fmt.Sprintf("%s (harness exit status %d)", exit.Reason, exit.ExitCode)
	}
}

const (
	lastOutputLines = 5
	lastOutputBytes = 1500
)

// LogSize is the size of the log of the shim in dir.
func LogSize(dir string) int64 {
	info, err := os.Stat(filepath.Join(dir, acphost.LogFile))
	if err != nil {
		return 0
	}
	return info.Size()
}

// LastOutput is the end of what the harness of session wrote to standard
// error after byte offset from of the log in dir: a few lines of bounded
// size. The shim logs to the same file, and its own lines name the session.
func LastOutput(dir, session string, from int64) string {
	content, err := os.ReadFile(filepath.Join(dir, acphost.LogFile))
	if err != nil || from < 0 || from > int64(len(content)) {
		return ""
	}
	var lines []string
	for line := range strings.SplitSeq(string(content[from:]), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.Contains(line+" ", " session="+session+" ") {
			lines = append(lines, line)
		}
	}
	text := strings.Join(lines[max(0, len(lines)-lastOutputLines):], "\n")
	if len(text) > lastOutputBytes {
		text = strings.ToValidUTF8(text[len(text)-lastOutputBytes:], "")
	}
	return text
}

// MaybeChild must be the first call in the shim command's main. When the
// process is the detached shim, it runs the shim and never returns.
func MaybeChild() { acphost.MaybeChild() }

// Main is the p7acpshim command. It returns the exit status.
func Main(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	flags := flag.NewFlagSet(Binary+" "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "shim directory, which holds spec.json")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(stderr, Binary+": --dir is required")
		return 2
	}

	var err error
	switch args[0] {
	case "start":
		var detached *proc.Detached
		detached, err = acphost.StartDetached(*dir, nil)
		if err == nil {
			fmt.Fprintln(stdout, detached.Pid)
		}
	case "run":
		err = acphost.Main(*dir)
	case "stop":
		err = Stop(context.Background(), *dir)
	case "status":
		if acphost.Alive(*dir) {
			fmt.Fprintln(stdout, "running")
		} else {
			fmt.Fprintln(stdout, "stopped", ExitReason(*dir))
		}
	default:
		usage(stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintf(stderr, "%s %s: %v\n", Binary, args[0], err)
		return 1
	}
	return 0
}

func usage(stderr io.Writer) {
	fmt.Fprintf(stderr, `usage: %s <command> --dir DIR

  start   start a detached shim for DIR and print its pid once the harness runs
  run     run the shim for DIR in the foreground
  stop    stop the shim of DIR and wait for it
  status  print whether a shim runs for DIR
`, Binary)
}

// RuntimeFiles are the files in a shim directory that belong to a running
// shim and mean nothing after it: its socket, its process ID and lock, and
// the record of how it exited. A copy of the directory leaves them out.
func RuntimeFiles() []string {
	return []string{acphost.ACPSocket, proc.SupervisorPidFile, proc.LockFile, acphost.ExitFile}
}
