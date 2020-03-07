package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//if strings.Contains(format, "update term from") {
		//	return
		//}
		log.Printf(format, a...)
	}
	return
}

type IntSlice []int

func (s IntSlice) Len() int { return len(s) }

func (s IntSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s IntSlice) Less(i, j int) bool { return s[i] < s[j] }

func min(a ...int) int {
	tmp := a[0]

	for i := range a {
		if a[i] < tmp {
			tmp = a[i]
		}
	}

	return tmp
}
