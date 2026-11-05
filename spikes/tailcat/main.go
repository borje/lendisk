package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
)

const usage = `usage: tcspike <command> [flags]

  derp   -region-out FILE              run a local DERP+STUN relay, write its region
  init   -state DIR [-region-file FILE | -region-id N]
                                        create a key and save a fixed region
  serve  -state DIR [-deny-all] [-nil-hook] [-v]
                                        run the tunnel server on port 9000
  dial   -state DIR -addr tc... [-msg S] [-v]
                                        dial the server's port 9000 and send S
  pubkey -state DIR                     print this node's public key
  decode tc...                          print what an address carries
`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "derp":
		err = cmdDERP(args)
	case "init":
		err = cmdInit(args)
	case "serve":
		err = cmdServe(args)
	case "dial":
		err = cmdDial(args)
	case "pubkey":
		err = cmdPubkey(args)
	case "decode":
		err = cmdDecode(args)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		log.Fatalf("%s: %v", cmd, err)
	}
}

func quietLogf(verbose bool) logger.Logf {
	if verbose {
		return log.Printf
	}
	return logger.Discard
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

func cmdDERP(args []string) error {
	fs := flag.NewFlagSet("derp", flag.ExitOnError)
	out := fs.String("region-out", "", "file to write the region JSON to")
	fs.Parse(args)
	if *out == "" {
		return errors.New("-region-out is required")
	}
	d, err := StartLocalDERP(logger.Discard)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := writeJSON(*out, d.Region); err != nil {
		return err
	}
	log.Printf("local DERP on 127.0.0.1:%d, STUN on :%d, region written to %s",
		d.Region.Nodes[0].DERPPort, d.Region.Nodes[0].STUNPort, *out)
	ctx, cancel := signalContext()
	defer cancel()
	<-ctx.Done()
	return nil
}

// cmdInit is lendisk init's tunnel part: generate the identity key and
// fix the bootstrap region once, saving the whole region (not just its
// ID) so the address does not move when the DERP map changes.
func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	state := fs.String("state", "", "state directory")
	regionFile := fs.String("region-file", "", "use this region JSON (e.g. from tcspike derp)")
	regionID := fs.Int("region-id", 0, "use this region of tailcat's default DERP map; 0 picks the nearest")
	fs.Parse(args)
	if *state == "" {
		return errors.New("-state is required")
	}
	if err := os.MkdirAll(*state, 0o700); err != nil {
		return err
	}
	keyPath := filepath.Join(*state, "key")
	if _, err := os.Stat(keyPath); err == nil {
		return fmt.Errorf("%s exists", keyPath)
	}
	k := key.NewNode()
	kt, _ := k.MarshalText()
	if err := os.WriteFile(keyPath, kt, 0o600); err != nil {
		return err
	}

	var reg *tailcfg.DERPRegion
	if *regionFile != "" {
		if err := readJSON(*regionFile, &reg); err != nil {
			return err
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		dm, err := tailcat.FetchDERPMap(ctx, tailcat.ExpandForServer)
		if err != nil {
			return err
		}
		id := tailcfg.DERPRegionID(*regionID)
		if id == 0 {
			if id, err = tailcat.PickBestRegion(ctx, dm); err != nil {
				return err
			}
		}
		reg = dm.Regions[id]
		if reg == nil {
			return fmt.Errorf("no region %d in %s", id, tailcat.DefaultDERPMapURL)
		}
	}
	if err := writeJSON(filepath.Join(*state, "region.json"), reg); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*state, "allowed"), nil, 0o600); err != nil {
		return err
	}
	fmt.Printf("public key %v\nregion %d (%s)\n", k.Public(), reg.RegionID, reg.RegionCode)
	return nil
}

type nodeState struct {
	key    key.NodePrivate
	region *tailcfg.DERPRegion
}

func loadState(dir string) (nodeState, error) {
	var st nodeState
	kt, err := os.ReadFile(filepath.Join(dir, "key"))
	if err != nil {
		return st, err
	}
	if err := st.key.UnmarshalText(kt); err != nil {
		return st, err
	}
	if err := readJSON(filepath.Join(dir, "region.json"), &st.region); err != nil {
		return st, err
	}
	return st, nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	state := fs.String("state", "", "state directory")
	denyAll := fs.Bool("deny-all", false, "use a hook that rejects every key")
	nilHook := fs.Bool("nil-hook", false, "pass a nil hook (the guard must refuse)")
	verbose := fs.Bool("v", false, "show tailcat's log")
	fs.Parse(args)
	st, err := loadState(*state)
	if err != nil {
		return err
	}
	var admit func(key.NodePublic) bool
	switch {
	case *nilHook:
		admit = nil
	case *denyAll:
		admit = func(k key.NodePublic) bool {
			log.Printf("admit %v: reject (deny-all)", k.ShortString())
			return false
		}
	default:
		admit = FileAdmitter{Path: filepath.Join(*state, "allowed"), Logf: log.Printf}.Admit
	}

	ctx, cancel := signalContext()
	defer cancel()
	s, err := StartServer(ctx, ServerConfig{Key: st.key, Region: st.region, Admit: admit, Logf: quietLogf(*verbose)})
	if err != nil {
		return err
	}
	defer s.Close()
	ln, err := s.Listen(ctx, "tcp", fmt.Sprintf(":%d", TunnelPort))
	if err != nil {
		return err
	}
	fmt.Printf("address %s\n", s.TailcatAddr())
	log.Printf("serving tunnel port %d as %v", TunnelPort, st.key.Public().ShortString())
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go serveConn(s, c)
	}
}

func serveConn(s *tailcat.Server, c net.Conn) {
	defer c.Close()
	k, err := PeerOf(s, c)
	if err != nil {
		log.Printf("closing %v before reading: %v", c.RemoteAddr(), err)
		return
	}
	log.Printf("accepted %v, peer key %v", c.RemoteAddr(), k)
	sc := bufio.NewScanner(c)
	for sc.Scan() {
		log.Printf("from %v: %q", k.ShortString(), sc.Text())
		fmt.Fprintf(c, "you are %v\n", k)
	}
	log.Printf("closed %v: %v", k.ShortString(), sc.Err())
}

func cmdDial(args []string) error {
	fs := flag.NewFlagSet("dial", flag.ExitOnError)
	state := fs.String("state", "", "state directory")
	addr := fs.String("addr", "", "server's tc... address")
	msg := fs.String("msg", "hello", "line to send")
	timeout := fs.Duration("timeout", 20*time.Second, "dial timeout")
	verbose := fs.Bool("v", false, "show tailcat's log")
	fs.Parse(args)
	st, err := loadState(*state)
	if err != nil {
		return err
	}
	c := &tailcat.Client{Server: tailcat.Addr(*addr), Key: st.key, Logf: quietLogf(*verbose)}
	defer c.Close()
	log.Printf("dialing as %v", c.PublicKey())
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	conn, err := c.DialTCPPort(ctx, TunnelPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "%s\n", *msg); err != nil {
		return err
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	fmt.Print(line)
	return nil
}

func cmdPubkey(args []string) error {
	fs := flag.NewFlagSet("pubkey", flag.ExitOnError)
	state := fs.String("state", "", "state directory")
	fs.Parse(args)
	st, err := loadState(*state)
	if err != nil {
		return err
	}
	fmt.Println(st.key.Public())
	return nil
}

func cmdDecode(args []string) error {
	if len(args) != 1 {
		return errors.New("want one address")
	}
	raw, err := tailcat.ParseAddrRaw(tailcat.Addr(args[0]))
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(raw, "", "  ")
	fmt.Println(string(b))
	return nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
