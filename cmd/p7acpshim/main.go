// Command p7acpshim is a thin entry point. The shim lives in
// pkg/execution/acpshim.
package main

import (
	"os"

	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
)

func main() {
	// First: a detached shim is this binary started again.
	acpshim.MaybeChild()
	os.Exit(acpshim.Main(os.Args[1:], os.Stdout, os.Stderr))
}
