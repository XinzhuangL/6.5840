package mr

import (
	"container/list"
)

type Queue struct {
	items *list.List
}

func NewQueue() *Queue {
	return &Queue{items: list.New()}
}

func (q *Queue) Enqueue(value interface{}) {
	q.items.PushBack(value)
}

func (q *Queue) Dequeue() interface{} {
	front := q.items.Front()
	if front != nil {
		q.items.Remove(front)
		return front.Value
	}
	return nil
}

func (q *Queue) Empty() bool {
	return q.items.Len() == 0
}
