package main

import (
	"fmt"
	"time"
)

func main() {
	for {
		timer := time.After(250 * time.Millisecond)
		select {
		case <-timer:
			fmt.Println(1)
		}
	}
}
