package main

import (
	"fmt"
	"time"
)

var DebugEnabled = false

func SetDebug(v bool) {
	DebugEnabled = v
}

func debugLog(format string, a ...interface{}) {
	if DebugEnabled {
		fmt.Printf(format+"\n", a...)
	}
}

func (r *ResourceManagement) log(format string, a ...interface{}) {
	fmt.Printf("["+r.Dataset+"]["+time.Now().Format(time.RFC3339)+"]"+format, a...)
}
