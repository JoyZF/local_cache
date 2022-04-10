package main

import (
	"fmt"
	"testing"
)

func BenchmarkTest(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%s%s", "hello", "world")
	}
}

func BenchmarkTest2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = "hello" + "world"
	}
}
