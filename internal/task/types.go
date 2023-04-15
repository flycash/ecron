package task

type Type string

const (
	TypeHTTP = "http_task"
)

type EventType string

const (
	// EventTypePreempted 当前调度节点已经抢占了这个任务
	EventTypePreempted = "preempted"
	// EventTypeRunnable 已经到了可运行的时间点
	// 这个时候可能还在等待别的资源
	// 借鉴于进程调度中的概念
	EventTypeRunnable = "runnable"
	// EventTypeRunning 已经找到了目标节点，并且正在运行
	EventTypeRunning = "running"
	// EventTypeFailed 任务运行失败
	EventTypeFailed = "failed"
	// EventTypeSuccess 任务运行成功
	EventTypeSuccess = "success"
	EventTypeInit    = "init"
)

type Config struct {
	Name       string
	Cron       string
	Type       Type
	Parameters []byte
}

// 通过TaskId关联任务详情
// type Task struct {
//	Parameters
//	TaskId int64
//	Epoch  int64
// }

// Task 任务的抽象接口
// ID 是全局唯一标识，而 Config 中的 Name 是一种用户友好标识。
// 那么对于 Name 是否是全局唯一，这里并不做此规定，而是完全取决于具体的实现。
type Task interface {
	ID() int64
	Config() Config
}

type Event struct {
	Task
	Type EventType
}
