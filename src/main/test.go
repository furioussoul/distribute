package main

import (
	"log"
	"sort"
)

func TestUpdateCommitIndex() {
	copyMatchIndex := make([]int, 5)
	copyMatchIndex[0] = 1
	copyMatchIndex[1] = 56
	copyMatchIndex[2] = 18
	copyMatchIndex[3] = 4
	copyMatchIndex[4] = 10
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))

	N := copyMatchIndex[len(copyMatchIndex)/2]

	if N != 10 {
		log.Fatal("unexpected err")
	}
}

func TestSliceArray() {
	nextIndex := 5
	l := make([]int, 0)
	for i := 0; i < 10; i++ {
		l = append(l, i)
	}

	if l[nextIndex:][0] != 5 {
		log.Fatal("unexpected err")
	}
}

func main() {
	TestSliceArray()
}
