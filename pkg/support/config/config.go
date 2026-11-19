// Package config reads a command's settings from a YAML file.
//
// A deployment puts its settings in a file, usually a mounted ConfigMap, and
// starts the command with --config <file>. Each top-level key is a flag's
// long name without the dashes in front, and a repeatable flag takes a YAML
// list:
//
//	temporal-address: temporal-frontend:7233
//	model:
//	  - deepseek=opencode-go:deepseek-v4.1-flash
//	  - sonnet=claude-oauth:claude-sonnet-5-5
//
// A flag given on the command line wins over the file, and the file wins over
// the flag's environment variable and its default. A key that names no flag
// of the command is an error, so a misspelt setting does not go unnoticed.
package config

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/alecthomas/kong"
	"go.yaml.in/yaml/v3"
)

// Flag is the --config flag. Embed it in a command's root with
// `embed:""` and parse with Option, so that its file is read before the
// other flags are resolved.
type Flag struct {
	Config kong.ConfigFlag `name:"config" type:"path" placeholder:"FILE" help:"YAML file of settings: each key is a flag's name, and a repeatable flag takes a list. Flags on the command line win over it, and it wins over environment variables."`
}

// Option makes kong read --config files with YAML.
func Option() kong.Option {
	return kong.Configuration(YAML)
}

// YAML is a kong.ConfigurationLoader for a YAML mapping of flag names to
// values.
func YAML(r io.Reader) (kong.Resolver, error) {
	values := map[string]any{}
	if err := yaml.NewDecoder(r).Decode(&values); err != nil && err != io.EOF {
		return nil, fmt.Errorf("read YAML settings: %w", err)
	}
	return resolver{values: values}, nil
}

type resolver struct {
	values map[string]any
}

// Validate rejects a key that is not the name of a flag in the application.
func (r resolver) Validate(app *kong.Application) error {
	known := map[string]bool{}
	err := kong.Visit(app, func(node kong.Visitable, next kong.Next) error {
		if flag, ok := node.(*kong.Flag); ok {
			known[flag.Name] = true
		}
		return next(nil)
	})
	if err != nil {
		return err
	}
	var unknown []string
	for key := range r.values {
		if !known[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("settings file: no flag named %s", strings.Join(unknown, ", "))
}

// Resolve returns the file's value for a flag, or nil when it has none.
func (r resolver) Resolve(_ *kong.Context, _ *kong.Path, flag *kong.Flag) (any, error) {
	value, ok := r.values[flag.Name]
	if !ok || value == nil {
		return nil, nil
	}
	return value, nil
}
