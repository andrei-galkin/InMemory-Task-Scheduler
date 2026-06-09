# Scheduler

A small Go scheduler example that queues tasks by execution time and runs them in the background.

## Structure

- `main.go` - example program that schedules tasks, cancels one task before execution, and waits for completion.
- `code/scheduler.go` - scheduler implementation using a min-heap priority queue.
- `code/task.go` - task and priority queue definitions for heap management.

## Features

- Schedule tasks to execute at an absolute Unix timestamp.
- Cancel a pending task by ID before it runs.
- Run the scheduler loop in a background goroutine.

## Run

```bash
cd c:\dev\Scheduler
go run .
```

## Notes

- The scheduler uses second-level timestamps via `time.Unix`.
- `Cancel` looks up pending tasks by ID in $O(1)$ time and then removes them from the heap in $O(\log n)$ using the task's stored heap index.
- `Stop()` cancels the scheduler loop; active task goroutines are launched asynchronously.
