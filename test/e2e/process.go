//go:build integration

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// The account the binaries under test run as, standing in for the service
	// account of the production units.
	serviceUID = 65534
	serviceGID = 65534

	// startTimeout covers recovery before a process serves; docker-backend's
	// own startup budget is 30 seconds.
	startTimeout = 90 * time.Second
	// shutdownGrace is how long a SIGTERM'd process gets before SIGKILL.
	// docker-backend's own shutdown budget is 75 seconds.
	shutdownGrace = 100 * time.Second
)

// Process is one binary under test, run as a child of the test binary. Its
// output goes to a log file, attached to the test when the test fails.
type Process struct {
	t        *testing.T
	name     string
	logPath  string
	coverDir string

	// stop and exited are nil while the process is not running.
	stop   context.CancelFunc
	exited chan error
}

// NewProcess names a process. The name must be unique in the run: it names
// the log file under LogDirEnv and the coverage directory under CoverDirEnv.
func NewProcess(t *testing.T, name string) *Process {
	t.Helper()
	p := &Process{
		t:        t,
		name:     name,
		logPath:  filepath.Join(dirFromEnv(t, LogDirEnv), name+".log"),
		coverDir: filepath.Join(dirFromEnv(t, CoverDirEnv), name),
	}
	require.NoError(t, os.MkdirAll(p.coverDir, 0o700))
	GiveToServiceAccount(t, p.coverDir)
	return p
}

// Env is the whole environment of a process under test. Nothing inherited may
// override a config file, and the binaries find their tools through PATH.
func (p *Process) Env() []string {
	return []string{"PATH=" + os.Getenv("PATH"), "GOCOVERDIR=" + p.coverDir}
}

// Start runs the command build returns and waits until ready reports true.
// The command gets SIGTERM, not SIGKILL, when the process is stopped.
func (p *Process) Start(build func(ctx context.Context) *exec.Cmd, ready func() bool) {
	p.t.Helper()
	require.Nil(p.t, p.stop, "%s is already running", p.name)

	// World-readable: CI uploads the log as the unprivileged runner account.
	logFile, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644) //nolint:gosec // G302, G304: a test log at a fixture-built path
	require.NoError(p.t, err)
	defer func() { _ = logFile.Close() }()

	// Not the test's context: that one is canceled before cleanup runs, and
	// the process must outlive the test body until Stop.
	ctx, cancel := context.WithCancel(context.Background())
	cmd := build(ctx)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = shutdownGrace
	if err := cmd.Start(); err != nil {
		cancel()
		require.NoError(p.t, err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	p.stop, p.exited = cancel, exited

	deadline := time.After(startTimeout)
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case err := <-exited:
			p.stop, p.exited = nil, nil
			cancel()
			p.t.Fatalf("%s exited during startup: %v\n%s", p.name, err, tail(p.logPath))
		case <-deadline:
			p.t.Fatalf("%s did not serve within %s\n%s", p.name, startTimeout, tail(p.logPath))
		case <-poll.C:
			if ready() {
				return
			}
		}
	}
}

// Stop sends SIGTERM and waits for the process to exit. A no-op when the
// process is not running.
func (p *Process) Stop() {
	p.t.Helper()
	if p.stop == nil {
		return
	}
	p.stop()
	err := <-p.exited
	p.stop, p.exited = nil, nil
	// exec reports a zero exit after the cancel signal as the context's
	// error. Anything else is the process's own failure.
	assert.ErrorIs(p.t, err, context.Canceled, "%s did not shut down cleanly", p.name)
}

// LogOnFailure attaches the end of the process's log to a failed test.
func (p *Process) LogOnFailure() {
	if p.t.Failed() {
		p.t.Logf("%s log (%s):\n%s", p.name, p.logPath, tail(p.logPath))
	}
}

// AsServiceAccount makes a command run as the service account, with no
// capabilities.
func AsServiceAccount(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: serviceUID, Gid: serviceGID}}
}

// GiveToServiceAccount makes the service account the owner of each path. It
// does not descend into a directory.
func GiveToServiceAccount(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		require.NoError(t, os.Lchown(path, serviceUID, serviceGID))
	}
}
