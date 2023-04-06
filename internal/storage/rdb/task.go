package rdb

import "github.com/ecodeclub/ecron/internal/task"

// TaskInfo 是直接映射到数据库表结构的类型定义
type TaskInfo struct {
	Id          int64 `eorm:"auto_increment,primary_key"`
	Name        string
	Status      string
	Epoch       int64
	Cron        string
	Type        string
	Parameters  []byte
	OccupierId  string // 占有该任务的storage
	CandidateId string // 该任务的候选storage
	CreateTime  int64
	UpdateTime  int64
}

func (t TaskInfo) ID() int64 {
	return t.Id
}

func (t TaskInfo) Config() task.Config {
	return task.Config{
		Name:       t.Name,
		Cron:       t.Cron,
		Type:       t.Type,
		Parameters: t.Parameters,
	}
}

type TaskExecution struct {
	Id         int64 `eorm:"auto_increment,primary_key"`
	TaskId     int64
	Status     string
	CreateTime int64
	UpdateTime int64
}
