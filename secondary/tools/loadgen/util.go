package main

import "fmt"
import "time"
import "math/rand"

func ranget(starts, ends string) string {
	start, err := time.Parse(time.RFC3339, starts)
	if err != nil {
		panic(fmt.Errorf("parsing first argument %v: %v\n", starts, err))
	}
	end, err := time.Parse(time.RFC3339, ends)
	if err != nil {
		panic(fmt.Errorf("parsing second argument %v: %v\n", ends, err))
	}
	t := start.Add(time.Duration(rand.Int63n(int64(end.Sub(start)))))
	return t.Format(time.RFC3339)
}
