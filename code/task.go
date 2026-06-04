package scheduler

// Task represents the unit of work to be scheduled.
type Task struct {
	ID        string
	ExecuteAt int64 // UNIX timestamp in seconds
	Action    func()
	index     int // Internal field required by container/heap
}

// PriorityQueue implements heap.Interface and holds Tasks.
type PriorityQueue []*Task

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].ExecuteAt < pq[j].ExecuteAt
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Task)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Peek() *Task {
	if len(*pq) == 0 {
		return nil
	}
	return (*pq)[0]
}
