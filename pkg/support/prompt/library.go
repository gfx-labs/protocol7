package prompt

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"strings"
	"text/template"
)

// Library is a group of prompt parts loaded from markdown files, so an app
// defines its prompts in one place and builds from them by name.
//
// A part is a file: "general.md" is the part "general". A directory is a
// slot with variants: "voice/direct.md" and "voice/plain.md" are two variants
// of the part "voice". Pick chooses a slot's variant for one use, and
// "voice/default.md" is used when none is picked. That is how one review can
// post in one voice and an escalation in another from the same library.
type Library struct {
	layers []fs.FS
	picks  map[string]string
}

// partStub lets Load parse a part that calls {{part "name"}}; the real function
// is bound to a builder when the part is executed.
var partStub = template.FuncMap{"part": func(string) (string, error) { return "", nil }}

// Load reads the parts of each file system. A later one overrides parts of
// the same name in an earlier one, so an app can lay its own files over a
// shared set. A part includes another with {{part "name"}}, resolved through
// the same Pick and layers and filled from the same vars, so a prompt's
// structure can live in the markdown. A part that includes itself is an error.
// Every file must parse as a text/template, so a broken part fails
// at load and not when a prompt is built.
func Load(layers ...fs.FS) (*Library, error) {
	l := &Library{layers: layers}
	var errs []error
	for _, layer := range layers {
		err := fs.WalkDir(layer, ".", func(name string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || path.Ext(name) != ".md" {
				return err
			}
			data, err := fs.ReadFile(layer, name)
			if err != nil {
				return err
			}
			if _, err := template.New(name).Funcs(partStub).Parse(string(data)); err != nil {
				errs = append(errs, fmt.Errorf("prompt: %s: %w", name, err))
			}
			return nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("prompt: load: %w", err))
		}
	}
	return l, errors.Join(errs...)
}

// Pick returns a copy of the library that resolves slot to variant. The
// variant's file must exist when the part is used.
func (l *Library) Pick(slot, variant string) *Library {
	picks := maps.Clone(l.picks)
	if picks == nil {
		picks = map[string]string{}
	}
	picks[slot] = variant
	return &Library{layers: l.layers, picks: picks}
}

// Text is the raw text of a part, not executed. Use Builder.Part to add it to
// a prompt with its placeholders filled.
func (l *Library) Text(name string) (string, error) {
	candidates := []string{name + ".md", name + "/default.md"}
	if variant, ok := l.picks[name]; ok {
		candidates = []string{name + "/" + variant + ".md"}
	}
	for _, file := range candidates {
		for i := len(l.layers) - 1; i >= 0; i-- {
			data, err := fs.ReadFile(l.layers[i], file)
			if err == nil {
				return strings.TrimSpace(string(data)), nil
			}
			if !errors.Is(err, fs.ErrNotExist) {
				return "", fmt.Errorf("prompt: %s: %w", file, err)
			}
		}
	}
	return "", fmt.Errorf("prompt: no part %q (looked for %s)", name, strings.Join(candidates, ", "))
}

// Has says whether the library has a part, such as optional guidance for one
// repository.
func (l *Library) Has(name string) bool {
	_, err := l.Text(name)
	return err == nil
}

// New returns a builder over this library, whose Part adds its parts.
func (l *Library) New(vars Vars) *Builder {
	return &Builder{vars: vars, library: l}
}

// Part adds a part of the builder's library, executed as a template like
// Template. A missing part, or a builder made without a library, is an error
// reported by Err.
func (b *Builder) Part(name string) *Builder {
	if b.library == nil {
		b.errs = append(b.errs, fmt.Errorf("prompt: part %q: the builder has no library", name))
		return b
	}
	text, err := b.library.Text(name)
	if err != nil {
		b.errs = append(b.errs, err)
		return b
	}
	out, err := b.execute(text, []string{name})
	if err != nil {
		b.errs = append(b.errs, fmt.Errorf("prompt: part %q: %w", name, err))
		return b
	}
	return b.Raw(out)
}
