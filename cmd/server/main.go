// PMS snapshot-diff sync server: sync engine + web console.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"pmssync/internal/store"
	"pmssync/internal/syncer"
	"pmssync/internal/target"
	"pmssync/internal/web"
)

func main() {
	meta := flag.String("meta", "meta.db", "meta store (SQLite) path")
	dw := flag.String("target", "dw.duckdb", "DuckDB warehouse path")
	addr := flag.String("addr", ":8080", "web console listen address")
	truthDir := flag.String("truth-dir", "", "simulator truth dir (enables /api/sim/verify)")
	flag.Parse()

	st, err := store.Open(*meta)
	if err != nil {
		log.Fatalf("open meta store: %v", err)
	}
	defer st.Close()

	tg, err := target.Open(*dw)
	if err != nil {
		log.Fatalf("open duckdb: %v", err)
	}
	defer tg.Close()

	sy := syncer.New(st, tg)
	// crash recovery: continue batches interrupted mid-flight
	go sy.ResumeAll()

	srv, err := web.New(st, tg, sy, *truthDir)
	if err != nil {
		log.Fatalf("web: %v", err)
	}
	fmt.Printf("控制台: http://localhost%s\n", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}
