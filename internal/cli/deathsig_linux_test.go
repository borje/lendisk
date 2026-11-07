package cli

import (
	"os/exec"
	"syscall"
)

// dieWithTest has the kernel kill cmd when the test binary dies, so a
// test killed by its timeout or by hand leaves no daemon running.
func dieWithTest(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
