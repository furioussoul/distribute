package raft

import (
	"log"
	"strings"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		if strings.Contains(format, "update term from") {
			return
		}
		log.Printf(format, a...)
	}
	return
}
