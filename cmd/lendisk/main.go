// Command lendisk is the lendisk node: one binary for the daemon and its CLI.
package main

import (
	"os"

	"github.com/borje/lendisk/internal/cli"
)

func main() {
	os.Exit(cli.Main(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
