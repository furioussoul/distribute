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

func TestSlice3() {
	l := make([]int, 0)
	for i := 0; i < 10; i++ {
		l = append(l, i)
	}

	l = l[1:3]
	//切片，保留开始的，丢弃结束的
	for i := range l {
		log.Printf("%d\n", l[i])
	}
}

func TestMin() {
	arr := []int{41, 47, 39}
	if min(arr...) != 39 {
		log.Printf("err")
	}
}

func main() {
	TestMin()
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

func max(a ...int) int {
	tmp := a[0]

	for i := range a {
		if a[i] > tmp {
			tmp = a[i]
		}
	}

	return tmp
}
