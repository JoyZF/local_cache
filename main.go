package main

import (
	"fmt"
	"os"
)
import "runtime/trace"

func main() {
	a := 500 & 255
	fmt.Println(a)
	trace.Start(os.Stdout)
	defer trace.Stop()
}

func Test() {
	_ = 500 & 255
}

func Test2() {
	_ = 500 % 255
}
