package storage

import (
	"context"

	"github.com/ecodeclub/ecron/internal/task"
)

type EventType string

const (
	// EventTypePreempted 抢占了一个任务
	EventTypePreempted = "preempted"
	// EventTypeDeleted 某一个任务被删除了
	EventTypeDeleted   = "deleted"
	EventTypeCreated   = "created"
	EventTypeRunnable  = "runnable"
	EventTypeEnd       = "end"
	EventTypeDiscarded = "discarded"

	Stop = "stop"
)

type Storage interface {
	// Events
	// 实现者需要处理 taskEvents
	Events(taskEvents <-chan task.Event) (<-chan Event, error)
	Close() error

	Add(ctx context.Context, t task.Task) (int64, error)
	Update(ctx context.Context, t task.Task) error
}

type Event struct {
	Type EventType
	Task task.Task
}

type Status struct {
	ExpectStatus string
	UseStatus    string
}
