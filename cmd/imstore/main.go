// Command imstore is the erasure-coded, S3-compatible object storage server.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"imstore/internal/admin"
	"imstore/internal/config"
	"imstore/internal/erasure"
	"imstore/internal/index"
	"imstore/internal/lifecycle"
	"imstore/internal/repair"
	"imstore/internal/s3"
	"imstore/internal/store"
	"imstore/internal/volume"
	"imstore/internal/web"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// Volumes (simulated disks).
	vols := make([]*volume.Volume, 0, len(cfg.Volumes))
	for _, vc := range cfg.Volumes {
		v, err := volume.New(vc.ID, vc.Path, vc.Enclosure)
		if err != nil {
			log.Fatalf("volume %s: %v", vc.ID, err)
		}
		vols = append(vols, v)
	}
	vm := volume.NewManager(vols)

	// Metadata index.
	if err := os.MkdirAll(parentDir(cfg.MetaDB), 0o755); err != nil {
		log.Fatalf("meta db dir: %v", err)
	}
	idx, err := index.Open(cfg.MetaDB)
	if err != nil {
		log.Fatalf("open index: %v", err)
	}
	defer idx.Close()

	// Erasure codec and core store.
	codec, err := erasure.New(cfg.DataShards, cfg.ParityShards, cfg.ShardBytes())
	if err != nil {
		log.Fatalf("erasure codec: %v", err)
	}
	st := store.New(codec, vm, idx)

	// Repair manager with throttling and foreground-aware yielding.
	rm := repair.NewManager(
		st,
		cfg.Repair.MaxBytesPerSecond,
		time.Duration(cfg.Repair.IntervalSeconds)*time.Second,
		cfg.Repair.YieldWhenBusy,
	)
	rm.Start()
	defer rm.Stop()

	// Lifecycle runner.
	lc := lifecycle.NewRunner(st, time.Hour,
		time.Duration(cfg.MultipartMaxPartHours)*time.Hour)
	lc.Start()
	defer lc.Stop()

	// HTTP layer: S3 API and console share one listener.
	s3h := s3.NewHandler(st, cfg.Region, cfg.AccessKey, cfg.SecretKey)
	adminAPI := admin.NewAPI(cfg.ConsoleToken, cfg.ExternalURL, vm, rm, idx, lc, s3h, web.FS())
	handler := adminAPI.Mux(s3h)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 30 * time.Second,
	}

	go func() {
		log.Printf("imstore listening on %s (S3 endpoint %s, console %s/ui/)",
			cfg.Listen, cfg.ExternalURL, cfg.ExternalURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down ...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func parentDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == os.PathSeparator {
			if i == 0 {
				return "/"
			}
			return p[:i]
		}
	}
	return "."
}
