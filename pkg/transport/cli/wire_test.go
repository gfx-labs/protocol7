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
