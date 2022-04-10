package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime/trace"
	"sync"
	"syscall"
)

func main() {
	// 1. 创建trace持久化的文件句柄
	f, err := os.Create("mmaptrace.out")
	if err != nil {
	log.Fatalf("failed to create trace output file: %v", err)
	}
	defer func() {
	if err := f.Close(); err != nil {
	log.Fatalf("failed to close trace file: %v", err)
	}
	}()

	// 2. trace绑定文件句柄
	if err := trace.Start(f); err != nil {
	log.Fatalf("failed to start trace: %v", err)
	}
	defer trace.Stop()

	// 下面就是你的监控的程序
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		mmap, _ := syscall.Mmap(10000, 0, 1000000, 1, 1)
		for i := 0; i < 10000000; i++ {
			mmap = append(mmap, fmt.Sprintf("key-%d", i)...)
		}
		defer wg.Done()
	}()
	wg.Wait()
}

func blob(char byte, len int) []byte {
	return bytes.Repeat([]byte{char}, len)
}
