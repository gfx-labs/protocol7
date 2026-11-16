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
		"group with snapshots": &sessionCommand{
			Worker: workerCommand{StateDir: "/state", Snapshots: snapshotFlags{Endpoint: "http://s3:8333", Bucket: "b", Prefix: "sessions", PathStyle: true}},
			Group:  "0123456789abcdef", Queue: "p7-group-0123456789abcdef",
		},
		"pods with snapshots": &podsCommand{Namespace: "p7", Template: "/template.json", Container: "session", Snapshots: snapshotFlags{Bucket: "b"}},
		"pods":                &podsCommand{Namespace: "p7", Template: "/template.json", Container: "session"},
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

// A group's pod keeps each session's snapshot under the group's prefix, and
// a command names a session or a group and not both.
func TestAGroupPodSnapshotsUnderTheGroupsPrefix(t *testing.T) {
	command := &sessionCommand{
		Worker: workerCommand{StateDir: "/state", Snapshots: snapshotFlags{Bucket: "b", Prefix: "sessions"}},
		Group:  "0123456789abcdef", Queue: "p7-group-0123456789abcdef",
	}
	if _, err := command.options(); err != nil {
		t.Fatal(err)
	}
	if got, want := command.Worker.Snapshots.Prefix, "sessions/groups/0123456789abcdef"; got != want {
		t.Errorf("prefix %q, want %q", got, want)
	}
	if command.Worker.session != "" || command.Worker.Host != "group-0123456789abcdef" {
		t.Errorf("session %q, host %q: a group pod restores no session at start", command.Worker.session, command.Worker.Host)
	}
	for name, bad := range map[string]*sessionCommand{
		"both":                      {SessionID: "s1", Group: "0123456789abcdef", Queue: "q"},
		"neither":                   {Queue: "q"},
		"a group that is not a key": {Group: "../x", Queue: "q"},
	} {
		if _, err := bad.options(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
