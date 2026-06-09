package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

type Scheduler struct {
	mu         sync.Mutex
	pq         PriorityQueue
	tasks      map[string]*Task
	wakeupChan chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func New() *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	schedule := &Scheduler{
		pq:         make(PriorityQueue, 0),
		tasks:      make(map[string]*Task),
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

func (schedule *Scheduler) Schedule(id string, executeAt int64, action func()) error {
	schedule.mu.Lock()
	defer schedule.mu.Unlock()

	if _, exists := schedule.tasks[id]; exists {
		return fmt.Errorf("task %q is already scheduled", id)
	}

	task := &Task{
		ID:        id,
		ExecuteAt: executeAt,
		Action:    action,
	}
	heap.Push(&schedule.pq, task)
	schedule.tasks[id] = task

	if schedule.pq.Peek() == task {
		select {
		case schedule.wakeupChan <- struct{}{}:
		default:
		}
	}

	return nil
}

func (schedule *Scheduler) Cancel(id string) bool {
	schedule.mu.Lock()
	defer schedule.mu.Unlock()

	task, ok := schedule.tasks[id]
	if !ok || task == nil {
		return false
	}

	if task.index >= 0 {
		heap.Remove(&schedule.pq, task.index)
	}
	delete(schedule.tasks, id)
	return true
}

func (schedule *Scheduler) run() {
	defer schedule.wg.Done()
	timer := time.NewTimer(1 * time.Hour)
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
				// FIX 2: single delete here; Cancel() handles its own path
				delete(schedule.tasks, task.ID)
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
