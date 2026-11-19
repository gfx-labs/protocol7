package prompt

import (
	"strings"
	"testing"
)

func TestBuilder(t *testing.T) {
	b := New(Vars{"repo": "oku-trade/trade", "pr": 7, "paths": []string{"a.go", "b.go"}})
	b.Title("Review {{.repo}}#{{.pr}}")
	b.P(`
		First line of a paragraph,
		  indented continuation kept relative.
	`)
	b.Bullets(`
		one item
		two item
		  continued
	`)
	b.List("", "{{.not}} filled")
	b.When(false, func(b *Builder) { b.P("skipped") })
	b.Fence("text", "untrusted ``` fence {{.repo}}\n")
	b.Template("Glob {a,b} and JSON {\"k\":1} stay.{{range .paths}} {{.}}{{end}}")

	want := "# Review oku-trade/trade#7\n\n" +
		"First line of a paragraph,\n  indented continuation kept relative.\n\n" +
		"- one item\n- two item continued\n\n" +
		"- {{.not}} filled\n\n" +
		"```text\nuntrusted ''' fence {{.repo}}\n```\n\n" +
		"Glob {a,b} and JSON {\"k\":1} stay. a.go b.go\n"
	if got := b.String(); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	if err := b.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMissingPlaceholderIsReported(t *testing.T) {
	b := New(Vars{"repo": "oku-trade/trade"})
	b.P("Review {{.repo}} at {{.head}}")
	if err := b.Err(); err == nil || !strings.Contains(err.Error(), "head") {
		t.Fatalf("missing placeholder not reported: %v", err)
	}
	if got := b.String(); got != "Review {{.repo}} at {{.head}}\n" {
		t.Fatalf("text with a missing placeholder should be added unfilled, got %q", got)
	}
}

func TestTruncateKeepsRunes(t *testing.T) {
	if got := Truncate("héllo", 2); got != "h\n(cut)" {
		t.Fatalf("got %q", got)
	}
}
