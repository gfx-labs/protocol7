// Package prompt builds model prompts: markdown sections, paragraphs written as
// indented raw strings, bullet lists, fenced untrusted text and {name}
// placeholders, without strings.Builder and fmt.Fprintf boilerplate.
package prompt

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Vars fill {name} placeholders. A value is formatted with %v.
type Vars map[string]any

// Builder collects a prompt. The zero value is ready to use. Blocks are
// separated by one blank line, so callers never manage newlines.
type Builder struct {
	vars    Vars
	blocks  []string
	missing map[string]bool
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
// is dedented and trimmed, and its placeholders are filled.
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
// added when none remain. Items are not placeholder-filled: they are often
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
// untrusted text cannot close the block early. text is not placeholder-filled.
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

// Template adds a markdown document, typically embedded from a .md file, with
// its placeholders filled. Keep static prose (backticks included) in such
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

// Err names the placeholders that had no value. They are left in the text.
func (b *Builder) Err() error {
	if len(b.missing) == 0 {
		return nil
	}
	names := make([]string, 0, len(b.missing))
	for name := range b.missing {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Errorf("prompt: no value for %s", strings.Join(names, ", "))
}

func (b *Builder) block(text string) *Builder {
	b.blocks = append(b.blocks, text)
	return b
}

// placeholder is {name}: a letter, then letters, digits, dots or underscores.
// Anything else in braces (JSON, globs such as {a,b}) is left alone.
var placeholder = regexp.MustCompile(`\{([A-Za-z][A-Za-z0-9_.]*)\}`)

func (b *Builder) fill(text string) string {
	return placeholder.ReplaceAllStringFunc(text, func(match string) string {
		name := match[1 : len(match)-1]
		value, ok := b.vars[name]
		if !ok {
			if b.missing == nil {
				b.missing = map[string]bool{}
			}
			b.missing[name] = true
			return match
		}
		return fmt.Sprint(value)
	})
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
