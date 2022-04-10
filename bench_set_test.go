package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/coocood/freecache"
	"github.com/go-redis/redis/v8"
	"sync"
	"testing"
	"time"
)

func BenchmarkBigcache(b *testing.B) {
	m := blob('a', 1024)
	cache, _ := bigcache.NewBigCache(bigcache.Config{
		Shards:             1,
		LifeWindow:         100 * time.Second,
		MaxEntriesInWindow: 100,
		MaxEntrySize:       256,
		HardMaxCacheSize:   1,
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache.Set(fmt.Sprintf("key-%d", i), m)
	}
}

func BenchmarkFreecache(b *testing.B) {
	cacheSize := 100 * 1024 * 1024
	m := blob('a', 1024)
	cache := freecache.NewCache(cacheSize)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cache.Set([]byte(fmt.Sprintf("key-%d", i)), m, 100)
	}
}

func BenchmarkRedis(b *testing.B)  {
	c := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        []string{"127.0.0.1:6379"},
		DB:           0,
		Password:     "",
		MasterName:   "",
		DialTimeout:  time.Second * 100,
		ReadTimeout:  time.Second * 100,
		WriteTimeout: time.Second * 100,
		IdleTimeout:  time.Second * 100,
	})
	m := blob('a', 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Set(context.Background(),fmt.Sprintf("key-%d", i),m, 100 * time.Second)
	}
}

func BenchmarkSyncMap(b *testing.B) {
	var m  sync.Map
	m2 := blob('a', 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Store(fmt.Sprintf("key-%d", i),m2)
	}
}

func blob(char byte, len int) []byte {
	return bytes.Repeat([]byte{char}, len)
}
