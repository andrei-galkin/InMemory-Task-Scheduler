package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type Scheduler struct {
	mu         sync.Mutex
	pq         PriorityQueue
	wakeupChan chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func New() *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	schedule := &Scheduler{
		pq:         make(PriorityQueue, 0),
		wakeupChan: make(chan struct{}, 1),
		ctx:        ctx,
		cancel:     cancel,
	}

	heap.Init(&schedule.pq)
	return schedule
}

func (schedule *Scheduler) Start() {
	schedule.wg.Add(1)
	go schedule.run()
}

func (schedule *Scheduler) Stop() {
	schedule.cancel()
	schedule.wg.Wait()
}

func (schedule *Scheduler) Schedule(id string, executeAt int64, action func()) {
	schedule.mu.Lock()
	task := &Task{ // Found automatically in task.go
		ID:        id,
		ExecuteAt: executeAt,
		Action:    action,
	}
	heap.Push(&schedule.pq, task)

	isEarliest := schedule.pq.Peek() == task
	schedule.mu.Unlock()

	if isEarliest {
		select {
		case schedule.wakeupChan <- struct{}{}:
		default:
		}
	}
}

// Cancel removes a task from the queue by its ID before it executes.
func (schedule *Scheduler) Cancel(id string) bool {
	schedule.mu.Lock()
	defer schedule.mu.Unlock()

	// Find the task index by matching the ID
	targetIdx := -1
	for i, task := range schedule.pq {
		if task.ID == id {
			targetIdx = i
			break
		}
	}

	// If found, remove it from the heap safely
	if targetIdx != -1 {
		heap.Remove(&schedule.pq, targetIdx)
		return true
	}

	return false // Task wasn't found (already executed or wrong ID)
}

func (schedule *Scheduler) run() {
	defer schedule.wg.Done()
	var timer *time.Timer
	timer = time.NewTimer(1 * time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		schedule.mu.Lock()
		now := time.Now().Unix()
		nextTask := schedule.pq.Peek()

		var duration time.Duration
		hasTask := nextTask != nil

		if hasTask {
			if nextTask.ExecuteAt <= now {
				task := heap.Pop(&schedule.pq).(*Task)
				schedule.mu.Unlock()
				go task.Action()
				continue
			} else {
				duration = time.Duration(nextTask.ExecuteAt-now) * time.Second
			}
		}
		schedule.mu.Unlock()

		if hasTask {
			timer.Reset(duration)
		} else {
			timer.Reset(1 * time.Hour)
		}

		select {
		case <-schedule.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-schedule.wakeupChan:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}
