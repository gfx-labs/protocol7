// Command p7-worker is a thin entry point. It is the worker command of p7,
// for deployments that run the core worker as its own binary.
package main

import (
	"os"

	"github.com/gfx-labs/protocol7/pkg/transport/cli"
)

func main() { os.Exit(cli.WorkerMain(os.Args[1:])) }
