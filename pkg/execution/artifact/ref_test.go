package artifact

import "testing"

func TestRef(t *testing.T) {
	digest := digestOf("x")
	ref, err := ParseRef("p7://sha256:" + digest + "#mock/up.png")
	if err != nil || ref.Digest != digest || ref.Name != "mock/up.png" {
		t.Fatalf("%+v, %v", ref, err)
	}
	if ref.String() != "p7://sha256:"+digest+"#mock/up.png" {
		t.Fatal(ref.String())
	}
	for _, text := range []string{"p7://sha256:" + digest, "sha256:" + digest, digest, "  p7://sha256:" + digest + "\n"} {
		if got, err := ParseRef(text); err != nil || got.Digest != digest || got.Name != "" {
			t.Errorf("%q: %+v, %v", text, got, err)
		}
	}
	// A fragment that is not a name is dropped and does not fail the reference.
	if got, err := ParseRef("p7://sha256:" + digest + "#../x"); err != nil || got.Name != "" {
		t.Errorf("%+v, %v", got, err)
	}
	for _, text := range []string{"", "p7://sha256:abc", "p7://md5:" + digest, "p7://sha256:" + digest + "00", "p7://sha256:" + digest[:63] + "G"} {
		if _, err := ParseRef(text); err == nil {
			t.Errorf("%q was accepted", text)
		}
	}
}

func TestValidName(t *testing.T) {
	for _, name := range []string{"a", "ui/home.png", "ui-redesign/chats.png", "v1.2+x@y=z", "a/b/c"} {
		if err := ValidName(name); err != nil {
			t.Errorf("%q: %v", name, err)
		}
	}
	for _, name := range []string{"", "/a", "a/", "a//b", "..", "a/../b", ".hidden", "a/.b", "a b", "a\\b", "é"} {
		if ValidName(name) == nil {
			t.Errorf("%q was accepted", name)
		}
	}
}
