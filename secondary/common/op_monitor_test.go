package common

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestOperationsMonitorTimeout(t *testing.T) {
	m := NewOperationsMonitor("1", 3, 1, 0)

	time.Sleep(1 * time.Second)

	timeoutHappened := false
	doneCh := make(chan bool)
	op := NewOperation(time.Duration(2)*time.Millisecond, doneCh,
		func(elapsed time.Duration) {
			timeoutHappened = true
			fmt.Printf("elapsed = %v\n", elapsed)
		},
	)
	success := m.AddOperation(op)
	if !success {
		t.Fatalf("AddOperation Failed")
	}

	time.Sleep(10 * time.Millisecond)
	close(doneCh)

	if timeoutHappened == false {
		t.Fatalf("%v", errors.New("Timeout was expected. Did not happen."))
	}
	m.Close()
}

func TestOperationsMonitorNoTimeout(t *testing.T) {
	m := NewOperationsMonitor("2", 3, 1, 0)

	time.Sleep(1 * time.Second)

	timeoutHappened := false
	doneCh := make(chan bool)
	op := NewOperation(time.Duration(4)*time.Millisecond, doneCh,
		func(elapsed time.Duration) {
			timeoutHappened = true
			fmt.Printf("elapsed = %v\n", elapsed)
		},
	)
	success := m.AddOperation(op)
	if !success {
		t.Fatalf("AddOperation Failed")
	}

	time.Sleep(2 * time.Millisecond)
	close(doneCh)

	time.Sleep(3 * time.Millisecond)
	if timeoutHappened == true {
		t.Fatalf("%v", errors.New("Timeout was expected. Did not happen"))
	}
	m.Close()
}

func TestOperationsMonitorBlocked(t *testing.T) {
	sz := 3
	m := NewOperationsMonitor("3", sz, 1, 0)

	time.Sleep(1 * time.Second)

	pushCh := make(chan bool)
	go func() {
		for i := 0; i < sz+2; i++ {

			doneCh := make(chan bool)
			op := NewOperation(time.Duration(2)*time.Microsecond, doneCh,
				func(elapsed time.Duration) {})
			_ = m.AddOperation(op)
		}
		pushCh <- true
	}()

	time.Sleep(1 * time.Second)
	select {

	case <-pushCh:
		fmt.Println("Caller is not blocked")

	default:
		t.Fatalf("%v", errors.New("Caller seems to be blocked"))
	}
	m.Close()
}

func TestOperationsMonitorRepopulate(t *testing.T) {
	sz := 3
	m := NewOperationsMonitor("4", sz, 1, 0)

	time.Sleep(1 * time.Second)

	x := 0
	doneCh := make(chan bool)
	op := NewOperation(time.Duration(10)*time.Millisecond, doneCh,
		func(elapsed time.Duration) {
			x++
			fmt.Printf("elapsed = %v\n", elapsed)
		})
	success := m.AddOperation(op)
	if !success {
		t.Fatalf("AddOperation Failed")
	}

	time.Sleep(35 * time.Millisecond)
	close(doneCh)
	if x != 3 {
		t.Fatalf("Unexpected value of x %v", x)
	}
	m.Close()
}

func TestOperationsMonitorOncePerIter(t *testing.T) {
	m := NewOperationsMonitor("5", 3, 20, 0)

	time.Sleep(1 * time.Millisecond)

	x := 0
	doneCh := make(chan bool)
	op := NewOperation(time.Duration(3)*time.Millisecond, doneCh,
		func(elapsed time.Duration) {
			x++
			fmt.Printf("elapsed = %v\n", elapsed)
		})
	success := m.AddOperation(op)
	if !success {
		t.Fatalf("AddOperation Failed")
	}

	time.Sleep(35 * time.Millisecond)
	close(doneCh)
	if x != 1 {
		t.Fatalf("Unexpected value of x %v", x)
	}
	m.Close()
}

func TestOperationsMonitorBatchSize(t *testing.T) {
	m := NewOperationsMonitor("5", 5, 200, 2)

	time.Sleep(10 * time.Millisecond)

	x := 0
	for i := 0; i < 4; i++ {
		doneCh := make(chan bool)
		op := NewOperation(time.Duration(100)*time.Millisecond, doneCh,
			func(elapsed time.Duration) {
				x++
				fmt.Printf("elapsed = %v\n", elapsed)
			})
		success := m.AddOperation(op)
		if !success {
			t.Fatalf("AddOperation Failed")
		}
	}

	time.Sleep(300 * time.Millisecond)
	if x != 2 {
		t.Fatalf("Unexpected value of x %v", x)
	}

	time.Sleep(200 * time.Millisecond)
	if x != 4 {
		t.Fatalf("Unexpected value of x %v", x)
	}

	m.Close()
}

func TestOperationsMonitorAtScale(t *testing.T) {
	m := NewOperationsMonitor("6", 5000, 5, 0)
	time.Sleep(10 * time.Millisecond)

	// Add 1000 operations to the monitor every 10ms
	// Runs 500 times.
	for j := 0; j < 500; j++ {
		doneChs := make([]chan bool, 0, 1000)
		x := 0
		for i := 0; i < 1000; i++ {
			doneCh := make(chan bool)
			timeout := rand.Intn(10)
			op := NewOperation(time.Duration(2+timeout)*time.Millisecond, doneCh,
				func(elapsed time.Duration) {
					x++
				})
			success := m.AddOperation(op)
			if !success {
				// failure here means 5ms interval is not enough for this load
				t.Fatalf("AddOperation Failed")
			}
			doneChs = append(doneChs, doneCh)
		}

		for _, doneCh := range doneChs {
			close(doneCh)
		}

		fmt.Println("Iteration", j, "done.")
		time.Sleep(10 * time.Millisecond)
	}

	m.Close()
}
