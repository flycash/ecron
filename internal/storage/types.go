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
	// 实现者需要处理 taskEvents，同时需要返回本实现内部发的 Storage 事件
	// 实现者和调用者都不应该阻塞对方。
	// 即实现者要保证自己处理 taskEvents 的速率应该大于调用者发送 taskEvents 的速率
	// 而调用者则应该保证处理返回的事件快于实现者发送事件
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
