package cli

import (
	"log/slog"
	"testing"

	"go.uber.org/fx"
)

// fx resolves a command's components when the process starts, so a
// constructor that asks for something no option provides is not a compile
// error. This checks each command's graph without building or starting
// anything.
func TestEveryCommandHasWhatItsComponentsAskFor(t *testing.T) {
	commands := map[string]interface{ options() (fx.Option, error) }{
		"worker":                  &workerCommand{NATSURL: "nats://127.0.0.1:4222", StateDir: "/state"},
		"worker without a stream": &workerCommand{StateDir: "/state"},
		"dev":                     &devCommand{DataDir: "/data", NATSPort: 4222},
		"session":                 &sessionCommand{Worker: workerCommand{NATSURL: "nats://127.0.0.1:4222", StateDir: "/state"}, SessionID: "s1", Queue: "p7-session-s1"},
		"session with snapshots": &sessionCommand{
			Worker:    workerCommand{StateDir: "/state", Snapshots: snapshotFlags{Endpoint: "http://s3:8333", Bucket: "b", PathStyle: true, Exclude: []string{"node_modules", ""}}},
			SessionID: "s1", Queue: "p7-session-s1",
		},
		"pool worker with snapshots": &sessionCommand{
			Worker: workerCommand{StateDir: "/state", Snapshots: snapshotFlags{Endpoint: "http://s3:8333", Bucket: "b", Prefix: "sessions", PathStyle: true}},
			Pool:   "0123456789abcdef", WorkerNum: "2", Queue: "p7-pool-0123456789abcdef-2",
		},
		"pods with snapshots": &podsCommand{Namespace: "p7", Template: "/template.json", Container: "session", Snapshots: snapshotFlags{Bucket: "b"}},
		"pods":                &podsCommand{Namespace: "p7", Template: "/template.json", Container: "session"},
		"files":               &filesCommand{StateDir: "/state"},
		// The operator credential's writer alone, and beside the artifacts.
		"files with the operator credential":               &filesCommand{StateDir: "/state", OperatorCredentials: true, Host: "h"},
		"files with the operator credential and artifacts": &filesCommand{StateDir: "/state", OperatorCredentials: true, Host: "h", Artifacts: artifactFlags{Dir: t.TempDir(), Retention: "24h"}},
	}
	for name, command := range commands {
		options, err := command.options()
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		// p7fx.Run supplies the logger.
		if err := fx.ValidateApp(fx.NopLogger, fx.Supply(slog.Default()), options); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A pool worker keeps each session's snapshot under the pool's prefix, serves
// the queue of its own slot and no other, and a command names a session or a
// pool and not both.
func TestAPoolWorkerSnapshotsUnderThePoolsPrefix(t *testing.T) {
	command := &sessionCommand{
		Worker: workerCommand{StateDir: "/state", Snapshots: snapshotFlags{Bucket: "b", Prefix: "sessions"}},
		Pool:   "0123456789abcdef", WorkerNum: "3", Queue: "p7-pool-0123456789abcdef-3",
	}
	if _, err := command.options(); err != nil {
		t.Fatal(err)
	}
	if got, want := command.Worker.Snapshots.Prefix, "sessions/pools/0123456789abcdef"; got != want {
		t.Errorf("prefix %q, want %q", got, want)
	}
	if command.Worker.session != "" || command.Worker.Host != "pool-0123456789abcdef-3" {
		t.Errorf("session %q, host %q: a pool worker restores no session at start", command.Worker.session, command.Worker.Host)
	}
	for name, bad := range map[string]*sessionCommand{
		"both":                  {SessionID: "s1", Pool: "0123456789abcdef", WorkerNum: "1", Queue: "q"},
		"neither":               {Queue: "q"},
		"a pool that is no key": {Pool: "../x", WorkerNum: "1", Queue: "q"},
		"no slot":               {Pool: "0123456789abcdef", Queue: "p7-pool-0123456789abcdef-1"},
		"a slot too high":       {Pool: "0123456789abcdef", WorkerNum: "17", Queue: "p7-pool-0123456789abcdef-17"},
		"another slot's queue":  {Pool: "0123456789abcdef", WorkerNum: "1", Queue: "p7-pool-0123456789abcdef-2"},
	} {
		if _, err := bad.options(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
