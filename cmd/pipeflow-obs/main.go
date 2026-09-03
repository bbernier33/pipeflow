package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	obshttp "github.com/bbernier33/pipeflow/obs/http"
	"github.com/bbernier33/pipeflow/obs/tui"
)

func main() {
	var baseURL, tokenEnv string
	var refresh, timeout time.Duration
	var once, color, clear bool
	flag.StringVar(&baseURL, "url", "http://127.0.0.1:8080/operations", "Pipeflow Observation HTTP base URL")
	flag.StringVar(&tokenEnv, "token-env", "PIPEFLOW_OBS_TOKEN", "environment variable containing the bearer token")
	flag.DurationVar(&refresh, "refresh", 2*time.Second, "dashboard refresh interval")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "HTTP request timeout")
	flag.BoolVar(&once, "once", false, "render one frame and exit")
	flag.BoolVar(&color, "color", true, "use ANSI colors")
	flag.BoolVar(&clear, "clear", true, "clear the terminal between frames")
	flag.Parse()
	client := obshttp.Client{BaseURL: baseURL, HTTPClient: &http.Client{Timeout: timeout}, Token: os.Getenv(tokenEnv)}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if once {
		dashboard, err := client.Fetch(ctx)
		if err == nil {
			err = tui.Render(os.Stdout, dashboard, tui.Options{Color: color})
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := tui.Run(ctx, os.Stdout, client, tui.Options{Refresh: refresh, Color: color, ClearScreen: clear}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
