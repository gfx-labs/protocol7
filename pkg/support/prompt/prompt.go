// Package prompt builds model prompts: markdown sections, paragraphs written as
// indented raw strings, bullet lists, fenced untrusted text and text/template
// placeholders ({{.name}}), without strings.Builder and fmt.Fprintf
// boilerplate.
package prompt

import (
	"errors"
	"fmt"
	"strings"
	"text/template"
)

// Vars is the data for {{.name}} placeholders and any other text/template
// actions ({{if .x}}, {{range .items}}).
type Vars map[string]any

// Builder collects a prompt. The zero value is ready to use. Blocks are
// separated by one blank line, so callers never manage newlines.
type Builder struct {
	vars   Vars
	blocks []string
	errs   []error
}

// New returns a builder whose placeholders are filled from vars.
func New(vars Vars) *Builder { return &Builder{vars: vars} }

// Set adds or replaces a placeholder value.
func (b *Builder) Set(name string, value any) *Builder {
	if b.vars == nil {
		b.vars = Vars{}
	}
	b.vars[name] = value
	return b
}

// Title starts a top-level part: "# text". Embedded documents with "##"
// sections sit below it.
func (b *Builder) Title(text string) *Builder {
	return b.block("# " + b.fill(text))
}

// Heading starts a section: "## text".
func (b *Builder) Heading(text string) *Builder {
	return b.block("## " + b.fill(text))
}

// P adds a paragraph. text may be an indented raw string spanning lines: it
// is dedented and trimmed, then executed as a template.
func (b *Builder) P(text string) *Builder {
	if text = b.fill(Dedent(text)); text != "" {
		b.blocks = append(b.blocks, text)
	}
	return b
}

// Bullets adds one list item per non-empty line of text (after dedent). A
// line that starts with whitespace continues the item above it.
func (b *Builder) Bullets(text string) *Builder {
	var items []string
	for _, line := range strings.Split(b.fill(Dedent(text)), "\n") {
		switch {
		case strings.TrimSpace(line) == "":
		case len(items) > 0 && (line[0] == ' ' || line[0] == '\t'):
			items[len(items)-1] += " " + strings.TrimSpace(line)
		default:
			items = append(items, strings.TrimSpace(line))
		}
	}
	return b.List(items...)
}

// List adds items as a bullet list. Empty items are skipped, and nothing is
// added when none remain. Items are not executed as templates: they are often
// data (paths, commands) that may contain braces.
func (b *Builder) List(items ...string) *Builder {
	var lines []string
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			lines = append(lines, "- "+item)
		}
	}
	if len(lines) > 0 {
		b.blocks = append(b.blocks, strings.Join(lines, "\n"))
	}
	return b
}

// Codes adds items as a bullet list of inline code.
func (b *Builder) Codes(items ...string) *Builder {
	quoted := make([]string, 0, len(items))
	for _, item := range items {
		if item != "" {
			quoted = append(quoted, "`"+item+"`")
		}
	}
	return b.List(quoted...)
}

// Fence adds text in a fenced block. Fences inside text are defused, so
// untrusted text cannot close the block early. text is never executed as a
// template.
func (b *Builder) Fence(lang, text string) *Builder {
	return b.block("```" + lang + "\n" + strings.ReplaceAll(strings.TrimRight(text, "\n"), "```", "'''") + "\n```")
}

// Raw adds text as one block, unchanged.
func (b *Builder) Raw(text string) *Builder {
	if text = strings.TrimSpace(text); text != "" {
		b.blocks = append(b.blocks, text)
	}
	return b
}

// Template adds a markdown document, typically embedded from a .md file,
// executed as a text/template. Keep static prose (backticks included) in such
// files and build only the dynamic parts in Go.
func (b *Builder) Template(text string) *Builder {
	return b.Raw(b.fill(text))
}

// When runs add only if cond holds, keeping optional sections inline.
func (b *Builder) When(cond bool, add func(*Builder)) *Builder {
	if cond {
		add(b)
	}
	return b
}

// String is the prompt, ending in one newline.
func (b *Builder) String() string {
	if len(b.blocks) == 0 {
		return ""
	}
	return strings.Join(b.blocks, "\n\n") + "\n"
}

// Err reports every placeholder that had no value and every template that
// did not parse. Such text is added unfilled, so the problem stays visible.
func (b *Builder) Err() error {
	return errors.Join(b.errs...)
}

func (b *Builder) block(text string) *Builder {
	b.blocks = append(b.blocks, text)
	return b
}

// fill executes text as a text/template over the builder's vars. A missing
// key is an error, not "<no value>". Text without "{{" is returned as is, so
// braces in prose (JSON, globs such as {a,b}) need no escaping.
func (b *Builder) fill(text string) string {
	if !strings.Contains(text, "{{") {
		return text
	}
	tmpl, err := template.New("prompt").Option("missingkey=error").Parse(text)
	if err != nil {
		b.errs = append(b.errs, fmt.Errorf("prompt: %w", err))
		return text
	}
	var out strings.Builder
	if err := tmpl.Execute(&out, map[string]any(b.vars)); err != nil {
		b.errs = append(b.errs, fmt.Errorf("prompt: %w", err))
		return text
	}
	return out.String()
}

// Dedent removes the indentation common to all non-blank lines and trims
// leading and trailing blank lines, so a paragraph can be an indented raw
// string in Go source.
func Dedent(text string) string {
	lines := strings.Split(strings.Trim(text, "\n"), "\n")
	indent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		n := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent < 0 || n < indent {
			indent = n
		}
	}
	for i, line := range lines {
		if len(line) >= indent && indent > 0 {
			lines[i] = line[indent:]
		} else {
			lines[i] = strings.TrimLeft(line, " \t")
		}
		lines[i] = strings.TrimRight(lines[i], " \t")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// Truncate cuts s to n bytes on a rune boundary and marks the cut.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8Start(s[n]) {
		n--
	}
	return s[:n] + "\n(cut)"
}

func utf8Start(c byte) bool { return c&0xC0 != 0x80 }
