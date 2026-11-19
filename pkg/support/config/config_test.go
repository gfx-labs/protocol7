package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"

	"github.com/gfx-labs/protocol7/pkg/support/config"
)

type command struct {
	config.Flag `embed:""`

	Address string            `name:"address" env:"CONFIG_TEST_ADDRESS"`
	Models  []string          `name:"model" sep:";"`
	Wait    time.Duration     `name:"wait" default:"1s"`
	Debug   bool              `name:"debug"`
	Labels  map[string]string `name:"label"`
}

func parse(t *testing.T, file string, args ...string) (command, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}
	var got command
	parser, err := kong.New(&got, config.Option(), kong.Exit(func(int) { t.Fatal("exit") }))
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.Parse(append([]string{"--config", path}, args...))
	return got, err
}

func TestFileSetsFlagsAndCommandLineWins(t *testing.T) {
	t.Setenv("CONFIG_TEST_ADDRESS", "from-env:1")
	got, err := parse(t, `
address: from-file:1
model:
  - a=x,y
  - b=z
wait: 2m
debug: true
label:
  team: core
`, "--wait", "3s")
	if err != nil {
		t.Fatal(err)
	}
	if got.Address != "from-file:1" {
		t.Errorf("address = %q, want the file's over the environment's", got.Address)
	}
	if strings.Join(got.Models, "|") != "a=x,y|b=z" {
		t.Errorf("models = %q", got.Models)
	}
	if got.Wait != 3*time.Second {
		t.Errorf("wait = %s, want the command line's", got.Wait)
	}
	if !got.Debug || got.Labels["team"] != "core" {
		t.Errorf("debug = %v, labels = %v", got.Debug, got.Labels)
	}
}

func TestUnknownKeyIsAnError(t *testing.T) {
	_, err := parse(t, "adress: x\n")
	if err == nil || !strings.Contains(err.Error(), "adress") {
		t.Fatalf("err = %v, want one naming adress", err)
	}
}
