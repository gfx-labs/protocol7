package prompt

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestLibrary(t *testing.T) {
	shared := fstest.MapFS{
		"rules.md":         {Data: []byte("Rules for {{.repo}}.")},
		"voice/default.md": {Data: []byte("plain voice")},
		"voice/direct.md":  {Data: []byte("direct voice")},
	}
	app := fstest.MapFS{
		"rules.md": {Data: []byte("App rules for {{.repo}}.")},
	}
	lib, err := Load(shared, app)
	if err != nil {
		t.Fatal(err)
	}

	b := lib.New(Vars{"repo": "oku-trade/retail"}).Part("rules").Part("voice")
	if got, want := b.String(), "App rules for oku-trade/retail.\n\nplain voice\n"; got != want || b.Err() != nil {
		t.Fatalf("got %q (%v), want %q", got, b.Err(), want)
	}
	if got := lib.Pick("voice", "direct").New(nil).Part("voice").String(); got != "direct voice\n" {
		t.Errorf("picked variant: %q", got)
	}
	if got := lib.New(nil).Part("voice").String(); got != "plain voice\n" {
		t.Errorf("Pick changed the original library: %q", got)
	}

	for name, b := range map[string]*Builder{
		"missing part":    lib.New(nil).Part("nope"),
		"missing variant": lib.Pick("voice", "loud").New(nil).Part("voice"),
		"no library":      New(nil).Part("rules"),
	} {
		if b.Err() == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestLoadRejectsABrokenTemplate(t *testing.T) {
	_, err := Load(fstest.MapFS{"bad.md": {Data: []byte("{{if}}")}})
	if err == nil || !strings.Contains(err.Error(), "bad.md") {
		t.Fatalf("got %v", err)
	}
}

func TestPartIncludesParts(t *testing.T) {
	shared := fstest.MapFS{
		"main.md":          {Data: []byte("Write for {{.who}}.\n\n{{part \"voice\"}}\n\n{{part \"dest\"}}")},
		"voice/default.md": {Data: []byte("plain voice")},
		"voice/direct.md":  {Data: []byte("direct voice for {{.who}}")},
		"dest.md":          {Data: []byte("{{part \"voice\"}} again")},
		"a.md":             {Data: []byte("{{part \"b\"}}")},
		"b.md":             {Data: []byte("{{part \"a\"}}")},
		"self.md":          {Data: []byte("{{part \"nope\"}}")},
	}
	lib, err := Load(shared)
	if err != nil {
		t.Fatal(err)
	}
	got := lib.Pick("voice", "direct").New(Vars{"who": "ops"}).Part("main").String()
	want := "Write for ops.\n\ndirect voice for ops\n\ndirect voice for ops again\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	for _, name := range []string{"a", "self"} {
		if b := lib.New(nil).Part(name); b.Err() == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if b := New(nil).Template(`{{part "voice"}}`); b.Err() == nil {
		t.Error("part without a library: no error")
	}
}
