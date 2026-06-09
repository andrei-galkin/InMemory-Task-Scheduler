package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelRemovesPendingTask(t *testing.T) {
	s := New()

	s.Schedule("task-1", time.Now().Add(5*time.Minute).Unix(), func() {})
	s.Schedule("task-2", time.Now().Add(10*time.Minute).Unix(), func() {})

	if !s.Cancel("task-1") {
		t.Fatal("expected cancel to succeed for pending task")
	}
	if got := s.pq.Len(); got != 1 {
		t.Fatalf("expected 1 pending task after cancel, got %d", got)
	}
	if task := s.pq.Peek(); task == nil || task.ID != "task-2" {
		t.Fatalf("expected remaining task to be task-2, got %#v", task)
	}
	if _, exists := s.tasks["task-1"]; exists {
		t.Fatal("expected task-1 to be removed from the tasks map")
	}
}

func TestCancelUnknownIDReturnsFalse(t *testing.T) {
	s := New()
	s.Schedule("task-1", time.Now().Add(5*time.Minute).Unix(), func() {})

	if s.Cancel("task-999") {
		t.Fatal("expected cancel to return false for unknown ID")
	}
}

func TestCancelAlreadyExecutedTaskReturnsFalse(t *testing.T) {
	s := New()
	s.Start()
	defer s.Stop()

	done := make(chan struct{})
	s.Schedule("task-1", time.Now().Unix(), func() { close(done) })

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not execute within 2s")
	}

	if s.Cancel("task-1") {
		t.Fatal("expected cancel to return false for already-executed task")
	}
}

func TestCancelPreventsExecution(t *testing.T) {
	s := New()
	s.Start()
	defer s.Stop()

	var executed atomic.Bool
	s.Schedule("task-1", time.Now().Add(200*time.Millisecond).Unix(), func() {
		executed.Store(true)
	})

	s.Cancel("task-1")
	time.Sleep(500 * time.Millisecond)

	if executed.Load() {
		t.Fatal("cancelled task should not have executed")
	}
}

func TestTaskExecutesAtScheduledTime(t *testing.T) {
	s := New()
	s.Start()
	defer s.Stop()

	done := make(chan struct{})
	target := time.Now().Add(1 * time.Second).Unix()
	s.Schedule("task-1", target, func() { close(done) })

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not execute within 3s of scheduled time")
	}
}

func TestMultipleTasksExecuteInOrder(t *testing.T) {
	s := New()
	s.Start()
	defer s.Stop()

	var mu sync.Mutex
	order := []string{}
	record := func(id string) func() {
		return func() {
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
		}
	}

	now := time.Now().Unix()
	s.Schedule("task-3", now+3, record("task-3"))
	s.Schedule("task-1", now+1, record("task-1"))
	s.Schedule("task-2", now+2, record("task-2"))

	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	expected := []string{"task-1", "task-2", "task-3"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d tasks to run, got %d", len(expected), len(order))
	}
	for i, id := range expected {
		if order[i] != id {
			t.Errorf("position %d: expected %s, got %s", i, id, order[i])
		}
	}
}

func TestLateScheduledTaskExecutes(t *testing.T) {
	s := New()
	s.Start()
	defer s.Stop()

	// scheduler is already running with nothing queued; then a task is added
	time.Sleep(100 * time.Millisecond)

	done := make(chan struct{})
	s.Schedule("late-task", time.Now().Add(500*time.Millisecond).Unix(), func() { close(done) })

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("late-added task did not execute")
	}
}

func TestScheduleDuplicateIDReturnsError(t *testing.T) {
	s := New()

	if err := s.Schedule("task-1", time.Now().Add(5*time.Minute).Unix(), func() {}); err != nil {
		t.Fatalf("first schedule should succeed, got: %v", err)
	}
	if err := s.Schedule("task-1", time.Now().Add(10*time.Minute).Unix(), func() {}); err == nil {
		t.Fatal("expected error when scheduling duplicate ID")
	}
	// heap must still contain exactly one task
	if got := s.pq.Len(); got != 1 {
		t.Fatalf("expected 1 task in heap after duplicate rejection, got %d", got)
	}
}

func TestConcurrentScheduleAndCancel(t *testing.T) {
	s := New()
	s.Start()
	defer s.Stop()

	const n = 50
	var wg sync.WaitGroup
	var executed atomic.Int32

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("task-%d", i)
		ts := time.Now().Add(500 * time.Millisecond).Unix()
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			s.Schedule(id, ts, func() { executed.Add(1) })
		}(id)
	}

	// cancel half of them concurrently while they're being scheduled
	for i := 0; i < n/2; i++ {
		id := fmt.Sprintf("task-%d", i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			s.Cancel(id)
		}(id)
	}

	wg.Wait()
	time.Sleep(2 * time.Second)

	// we can't assert an exact count because of scheduling races,
	// but the scheduler must not deadlock, panic, or execute more than n tasks
	if got := executed.Load(); got > n {
		t.Fatalf("executed %d tasks, expected at most %d", got, n)
	}
}

func TestStopDrainsGracefully(t *testing.T) {
	s := New()
	s.Start()

	var executed atomic.Bool
	s.Schedule("task-1", time.Now().Add(5*time.Minute).Unix(), func() {
		executed.Store(true)
	})

	s.Stop()

	if executed.Load() {
		t.Fatal("far-future task should not have executed before Stop returned")
	}
}
