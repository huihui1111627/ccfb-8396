// Simulator: builds fake store databases, mutates them round by round,
// drives the sync server over HTTP and verifies every synced version against
// the recorded ground truth.
//
// Usage:
//
//	simulator init   -dir demo -stores 6 -orders 50000 -meta demo/meta.db
//	simulator mutate -dir demo -meta demo/meta.db -round 1
//	simulator run    -dir demo -meta demo/meta.db -rounds 10 -server http://localhost:8080
//	simulator verify -dir demo -meta demo/meta.db -target demo/dw.duckdb
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"pmssync/internal/sim"
	"pmssync/internal/store"
	"pmssync/internal/target"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: simulator <init|mutate|run|verify> [flags]")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "init":
		fs := flag.NewFlagSet("init", flag.ExitOnError)
		dir := fs.String("dir", "demo", "demo data dir")
		stores := fs.Int("stores", 6, "number of store DBs")
		orders := fs.Int("orders", 50000, "orders per store")
		meta := fs.String("meta", "demo/meta.db", "meta.db path of the sync server")
		fs.Parse(os.Args[2:])
		must(sim.Init(sim.Spec{Dir: *dir, Stores: *stores, OrdersPerStore: *orders, MetaPath: *meta}))
		fmt.Printf("初始化完成: %d 门店 x %d 订单, 目录 %s\n", *stores, *orders, *dir)

	case "mutate":
		fs := flag.NewFlagSet("mutate", flag.ExitOnError)
		dir := fs.String("dir", "demo", "demo data dir")
		meta := fs.String("meta", "demo/meta.db", "meta.db path")
		round := fs.Int("round", 1, "round number")
		fs.Parse(os.Args[2:])
		must(sim.Mutate(sim.Spec{Dir: *dir, MetaPath: *meta}, *round))
		fmt.Printf("第 %d 轮变更已应用并记录真实日志\n", *round)

	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		dir := fs.String("dir", "demo", "demo data dir")
		meta := fs.String("meta", "demo/meta.db", "meta.db path")
		rounds := fs.Int("rounds", 10, "number of rounds")
		server := fs.String("server", "http://localhost:8080", "sync server URL")
		fs.Parse(os.Args[2:])
		runRounds(*dir, *meta, *server, *rounds)

	case "verify":
		fs := flag.NewFlagSet("verify", flag.ExitOnError)
		dir := fs.String("dir", "demo", "demo data dir")
		meta := fs.String("meta", "demo/meta.db", "meta.db path")
		dw := fs.String("target", "demo/dw.duckdb", "duckdb path")
		fs.Parse(os.Args[2:])
		st, err := store.Open(*meta)
		must(err)
		defer st.Close()
		tg, err := target.Open(*dw)
		must(err)
		defer tg.Close()
		rep, err := sim.Verify(st, tg, *dir)
		must(err)
		printReport(rep)

	default:
		fmt.Println("unknown command:", os.Args[1])
		os.Exit(1)
	}
}

func runRounds(dir, meta, server string, rounds int) {
	st, err := store.Open(meta)
	must(err)
	sources, err := st.ListSources()
	must(err)
	st.Close()
	if len(sources) == 0 {
		fmt.Println("没有已登记的源库, 先运行 simulator init")
		os.Exit(1)
	}
	syncAll := func() error {
		for _, src := range sources {
			var resp struct {
				OK      bool   `json:"ok"`
				BatchID int64  `json:"batch_id"`
				Error   string `json:"error"`
			}
			post(server+fmt.Sprintf("/api/sources/%d/sync", src.ID), &resp)
			if !resp.OK {
				return fmt.Errorf("门店%d 同步失败: %s", src.ID, resp.Error)
			}
		}
		return nil
	}
	verify := func() *sim.Report {
		var rep sim.Report
		post(server+"/api/sim/verify", &rep)
		return &rep
	}

	// round 0: initial full sync must land before any mutation
	fmt.Println("初始全量同步...")
	if err := syncAll(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if rep := verify(); !rep.OK {
		fmt.Println("初始同步校验失败:")
		printReport(rep)
		os.Exit(1)
	}
	fmt.Println("初始同步校验通过")

	for round := 1; round <= rounds; round++ {
		t0 := time.Now()
		must(sim.Mutate(sim.Spec{Dir: dir, MetaPath: meta}, round))
		if err := syncAll(); err != nil {
			fmt.Printf("第 %d 轮: %v\n", round, err)
			os.Exit(1)
		}
		rep := verify()
		status := "OK"
		if !rep.OK {
			status = "MISMATCH"
		}
		fmt.Printf("第 %d 轮: %s  键=%d 版本=%d 存活行=%d 耗时=%s\n",
			round, status, rep.KeysChecked, rep.VersionsChecked, rep.LiveChecked, time.Since(t0).Round(time.Millisecond))
		if !rep.OK {
			for _, m := range rep.Mismatches {
				fmt.Println("  ", m)
			}
			os.Exit(1)
		}
	}
	fmt.Println("全部轮次校验通过")
}

func printReport(rep *sim.Report) {
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if !rep.OK {
		os.Exit(1)
	}
}

func post(url string, out any) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(nil))
	if err != nil {
		fmt.Println("POST", url, ":", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, out); err != nil {
		fmt.Println("bad response:", string(body))
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		fmt.Println("错误:", err)
		os.Exit(1)
	}
}
