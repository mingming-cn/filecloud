// Command filecloud initializes and serves a single-node filecloud data directory.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mingming-cn/filecloud/internal/health"
	"github.com/mingming-cn/filecloud/internal/storage"
)

const (
	_defaultListen  = "127.0.0.1:8080"
	_shutdownPeriod = 5 * time.Second
)

func main() {
	os.Exit(mainCode())
}

func mainCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "filecloud:", err); writeErr != nil {
			return 1
		}
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: filecloud <init|serve> [options]")
	}
	switch args[0] {
	case "init":
		flags := flag.NewFlagSet("init", flag.ContinueOnError)
		flags.SetOutput(stderr)
		dataDir := flags.String("data-dir", "", "Filecloud data directory")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *dataDir == "" || flags.NArg() != 0 {
			return errors.New("usage: filecloud init --data-dir PATH")
		}
		return storage.Init(ctx, *dataDir)
	case "serve":
		flags := flag.NewFlagSet("serve", flag.ContinueOnError)
		flags.SetOutput(stderr)
		dataDir := flags.String("data-dir", "", "Filecloud data directory")
		listen := flags.String("listen", _defaultListen, "HTTP listen address")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *dataDir == "" || flags.NArg() != 0 {
			return errors.New("usage: filecloud serve --data-dir PATH [--listen ADDRESS]")
		}
		return serve(ctx, *dataDir, *listen, stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(ctx context.Context, dataDir, address string, stdout, stderr io.Writer) (retErr error) {
	store, err := storage.OpenForServe(ctx, dataDir)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, store.Close())
	}()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger := log.New(stderr, "filecloud: ", log.LstdFlags)
	server := &http.Server{
		Handler:           health.NewHandler(store.DB(), store.ObjectsDir(), logger),
		ErrorLog:          logger,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if _, err := fmt.Fprintf(stdout, "listening on %s\n", listener.Addr()); err != nil {
		return errors.Join(fmt.Errorf("write listen address: %w", err), listener.Close())
	}

	shutdownErr := make(chan error, 1)
	stopShutdown := context.AfterFunc(ctx, func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				shutdownErr <- fmt.Errorf("shutdown panic: %v", recovered)
			}
		}()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _shutdownPeriod)
		defer cancel()
		shutdownErr <- server.Shutdown(shutdownCtx)
	})
	serveErr := server.Serve(listener)
	if stopShutdown() {
		return fmt.Errorf("serve HTTP: %w", serveErr)
	}
	if err := <-shutdownErr; err != nil {
		return fmt.Errorf("shutdown HTTP: %w", err)
	}
	if !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", serveErr)
	}
	return nil
}
