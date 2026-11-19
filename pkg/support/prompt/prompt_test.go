package prompt

import (
	"strings"
	"testing"
)

func TestBuilder(t *testing.T) {
	b := New(Vars{"repo": "oku-trade/trade", "pr": 7})
	b.Title("Review {repo}#{pr}")
	b.P(`
		First line of a paragraph,
		  indented continuation kept relative.
	`)
	b.Bullets(`
		one item
		two item
		  continued
	`)
	b.List("", "{not filled}")
	b.When(false, func(b *Builder) { b.P("skipped") })
	b.Fence("text", "untrusted ``` fence\n")
	b.Template("Glob {a,b} and JSON {\"k\":1} stay. {missing} is reported.")

	want := "# Review oku-trade/trade#7\n\n" +
		"First line of a paragraph,\n  indented continuation kept relative.\n\n" +
		"- one item\n- two item continued\n\n" +
		"- {not filled}\n\n" +
		"```text\nuntrusted ''' fence\n```\n\n" +
		"Glob {a,b} and JSON {\"k\":1} stay. {missing} is reported.\n"
	if got := b.String(); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	if err := b.Err(); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing placeholder not reported: %v", err)
	}
}

func TestTruncateKeepsRunes(t *testing.T) {
	if got := Truncate("héllo", 2); got != "h\n(cut)" {
		t.Fatalf("got %q", got)
	}
}
