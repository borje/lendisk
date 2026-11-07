//go:build !linux

package cli

import "os/exec"

func dieWithTest(*exec.Cmd) {}
