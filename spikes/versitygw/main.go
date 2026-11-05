package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	store := flag.String("store", "./data/store", "posix store root")
	sock := flag.String("sock", "./data/run/gw.sock", "internal unix socket versitygw serves on")
	fwd := flag.String("forward", "127.0.0.1:9000", "TCP address forwarded to the socket (empty: none)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *fwd != "" {
		go func() {
			if err := forward(ctx, *fwd, *sock); err != nil {
				log.Fatalf("forward: %v", err)
			}
		}()
	}
	log.Printf("versitygw on unix:%s, store %s, forward %s", *sock, *store, *fwd)
	if err := runGateway(ctx, *store, *sock, log.Printf); err != nil {
		log.Fatal(err)
	}
}

// forward is the trivial stand-in for lendisk's bridge: a byte copy from
// a TCP listener to the unix socket. The real bridge serves HTTP and
// proxies per request; this one is enough to show the socket works for
// a backup tool.
func forward(ctx context.Context, addr, sock string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go func() {
			defer c.Close()
			u, err := net.Dial("unix", sock)
			if err != nil {
				log.Printf("forward dial: %v", err)
				return
			}
			defer u.Close()
			done := make(chan struct{}, 2)
			go func() { io.Copy(u, c); u.(*net.UnixConn).CloseWrite(); done <- struct{}{} }()
			go func() { io.Copy(c, u); c.(*net.TCPConn).CloseWrite(); done <- struct{}{} }()
			<-done
			<-done
		}()
	}
}
