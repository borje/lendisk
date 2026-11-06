//go:build admitall

package cli

// admitAll: this is the SEC-11 test build, whose tunnel admits every key
// so that the test shows the middleware alone refuses an unknown one.
// Never ship a binary built with the admitall tag.
const admitAll = true
