// Command p7 is a thin entry point. Its commands live in pkg/transport/cli.
package main

import (
	"os"

	"github.com/gfx-labs/protocol7/pkg/transport/cli"
)

func main() { os.Exit(cli.Main(os.Args[1:])) }
