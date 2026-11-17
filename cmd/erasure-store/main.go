// Command erasure-store 启动 S3 兼容纠删码对象存储服务。
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

	"erasurestore/internal/config"
	"erasurestore/internal/console"
	"erasurestore/internal/lifecycle"
	"erasurestore/internal/repair"
	"erasurestore/internal/s3"
	"erasurestore/internal/storage"
)

func main() {
	cfgPath := flag.String("config", "configs/erasure-store.json", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	log.Printf("纠删码配置: data=%d parity=%d stripe=%d 磁盘数=%d",
		cfg.DataShards, cfg.ParityShards, cfg.StripeSize, len(cfg.Disks))

	store, err := storage.New(cfg)
	if err != nil {
		log.Fatalf("初始化存储引擎失败: %v", err)
	}
	repairMgr := repair.NewManager(store, cfg.RepairBytesPerSec)
	repairMgr.Start()
	lcRunner := lifecycle.New(store)
	lcRunner.Start()

	s3Srv := &http.Server{
		Addr:              cfg.S3Addr,
		Handler:           s3.NewServer(store, cfg).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	consoleSrv := &http.Server{
		Addr:              cfg.ConsoleAddr,
		Handler:           console.New(store, repairMgr, lcRunner, cfg.ConsoleToken).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("S3 服务监听 %s（AccessKey=%s Region=%s）", cfg.S3Addr, cfg.AccessKey, cfg.Region)
		if err := s3Srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("S3 服务异常: %v", err)
		}
	}()
	go func() {
		log.Printf("管理控制台监听 %s（Token=%s）", cfg.ConsoleAddr, cfg.ConsoleToken)
		if err := consoleSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("控制台异常: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("收到退出信号，开始优雅关闭…")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s3Srv.Shutdown(ctx)
	_ = consoleSrv.Shutdown(ctx)
	repairMgr.Stop()
	lcRunner.Stop()
	if err := store.Meta().Close(); err != nil {
		log.Printf("关闭元数据库失败: %v", err)
	}
	log.Println("已退出")
}
