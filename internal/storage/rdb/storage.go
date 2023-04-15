// Copyright 2023 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rdb

import (
	"context"
	"github.com/ecodeclub/ecron/internal/logger"
	logs "github.com/ecodeclub/log-api"
	"log"
	"sync"
	"time"

	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/eorm"
	_ "github.com/go-sql-driver/mysql"
	shortuuid "github.com/lithammer/shortuuid/v4"
)

// Storage 基于关系型数据库的实现
type Storage struct {
	id                string // 唯一标识一个storage
	db                *eorm.DB
	preemptInterval   time.Duration         // 发起任务抢占时间间隔
	refreshInterval   time.Duration         // 发起任务抢占续约时间间隔
	preemptionTimeout time.Duration         // 抢占任务超时时间
	refreshRetry      storage.RetryStrategy // 续约重试策略
	events            chan storage.Event
	stop              chan struct{}
	closeOnce         sync.Once
	startOnce         sync.Once
	n                 int64 // 一次最多更新的候选者个数
	logger            logs.Logger
}

func NewStorage(db *eorm.DB, opt ...option.Option[Storage]) (*Storage, error) {
	s := &Storage{
		events:            make(chan storage.Event),
		db:                db,
		preemptInterval:   2 * time.Second,
		refreshInterval:   5 * time.Second,
		preemptionTimeout: time.Minute,
		refreshRetry: &storage.RefreshIntervalRetry{
			Interval: time.Second,
			Max:      3,
		},
		stop: make(chan struct{}),
		n:    3,
		// 默认使用全局的日志，将来可以考虑允许用户覆盖
		logger: logger.Default(),
	}
	option.Apply[Storage](s, opt...)
	s.id = shortuuid.New()
	return s, nil
}

// func WithPreemptInterval(t time.Duration) option.Option[Storage] {
// 	return func(s *Storage) {
// 		s.preemptInterval = t
// 	}
// }

// func WithRefreshRetry(r storage.RetryStrategy) option.Option[Storage] {
// 	return func(s *Storage) {
// 		s.refreshRetry = r
// 	}
// }

// func WithRefreshInterval(t time.Duration) option.Option[Storage] {
// 	return func(s *Storage) {
// 		s.refreshInterval = t
// 	}
// }

// func WithPreemptTimeout(t time.Duration) option.Option[Storage] {
// 	return func(s *Storage) {
// 		s.preemptionTimeout = t
// 	}
// }

// Add 创建task，设置调度状态是created
func (s *Storage) Add(ctx context.Context, t task.Task) (int64, error) {
	cfg := t.Config()
	id, err := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
		Name:       cfg.Name,
		Cron:       cfg.Cron,
		Status:     storage.EventTypeCreated,
		Type:       cfg.Type,
		Parameters: cfg.Parameters,
		CreateTime: time.Now().Unix(),
		UpdateTime: time.Now().Unix(),
	}).Exec(ctx).LastInsertId()
	if err != nil {
		return -1, errs.NewAddTaskError(err)
	}
	return id, nil
}

// AddExecution 创建一条执行记录
// func (s *Storage) AddExecution(ctx context.Context, taskId int64) (int64, error) {
//	id, err := eorm.NewInserter[TaskExecution](s.db).Values(&TaskExecution{
//		ExecuteStatus: task.EventTypeInit,
//		TaskId:        taskId,
//		CreateTime:    time.Now().Unix(),
//		UpdateTime:    time.Now().Unix(),
//	}).Exec(ctx).LastInsertId()
//	if err != nil {
//		return -1, errs.NewAddTaskError(err)
//	}
//	return id, nil
// }

func (s *Storage) casTaskStatus(ctx context.Context, taskId int64, oldStatus, newStatus string) error {
	cond := eorm.C("Id").EQ(taskId).And(eorm.C("status").EQ(oldStatus))
	// 要将占有者 ID 修改为自己，并且将候选者 ID 清空。
	// 候选者（如果这个候选者不是自己）需要下一轮继续比较负载来确定要不要发起抢占
	ra, err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		Status:      newStatus,
		OccupierId:  s.id,
		CandidateId: "",
		UpdateTime:  time.Now().Unix(),
	}).Set(eorm.Columns("Status", "UpdateTime", "OccupierId", "CandidateId")).
		Where(cond).Exec(ctx).RowsAffected()
	if err != nil {
		return errs.NewCASTaskStatusError(err)
	}
	// 抢占失败，应该是一个可以容忍的情况，这意味着出现了并发
	if ra == 0 {
		s.logger.Debug("ecron: 抢占失败，被别的 storage 实例抢占了。"+
			"taskId: %d, 当前 storage: %s", taskId, s.id)
	}
	return nil
}

// func (s *Storage) compareAndUpdateTaskExecutionStatus(ctx context.Context, taskId int64, old, new string) error {
// 	cond := eorm.C("TaskId").EQ(taskId).And(eorm.C("ExecuteStatus").EQ(old))
// 	ra, err := eorm.NewUpdater[TaskExecution](s.db).Update(&TaskExecution{
// 		Status:     new,
// 		UpdateTime: time.Now().Unix(),
// 	}).Set(eorm.Columns("ExecuteStatus", "UpdateTime")).Where(cond).Exec(ctx).RowsAffected()
// 	if err != nil {
// 		return errs.NewCASTaskStatusError(err)
// 	}
// 	if ra == 0 {
// 		return errs.NewCompareAndUpdateAffectZeroError()
// 	}
// 	return nil
// }

func (s *Storage) Update(ctx context.Context, t task.Task) error {
	cfg := t.Config()
	updateTask := &TaskInfo{
		Name:       cfg.Name,
		Cron:       cfg.Cron,
		Type:       cfg.Type,
		Parameters: cfg.Parameters,
		UpdateTime: time.Now().Unix(),
	}
	return eorm.NewUpdater[TaskInfo](s.db).
		Update(updateTask).SkipZeroValue().Where(eorm.C("Id").EQ(t.ID())).Exec(ctx).Err()
}

// preemptLoop 每隔固定时间去db中抢占任务
func (s *Storage) preemptLoop() {
	// 抢占任务间隔
	tickerP := time.NewTicker(s.preemptInterval)
	for {
		select {
		case <-tickerP.C:
			s.logger.Info("ecron: storage[%d] 开始进行任务抢占", s.id)
			// 这边不需要开启 goroutine。
			s.preempted()
		case <-s.stop:
			s.logger.Info("ecron: storage[%d] 停止任务抢占", s.id)
			return
		}
	}
}

// 抢占两种类型的任务：
// 1. 长时间没有续约的已经抢占的任务
// 2. 处于创建状态的任务
// 3. 占有者主动放弃(续约时会检查是否需要放弃)，且候选者是当前storage
func (s *Storage) preempted() {
	// TODO 是否可以提前计算这个 max refreshInterval。一个 Storage 创建好之后不应该再继续修改
	// +1 是为了防止边界情况
	maxRefreshInterval := (s.refreshRetry.GetMaxRetry() + 1) * int64(s.refreshInterval.Seconds())
	// 1. 长时间没有续约的已经抢占的任务
	cond1 := eorm.C("status").EQ(storage.EventTypePreempted).
		And(eorm.C("UpdateTime").LTEQ(time.Now().Unix() - maxRefreshInterval))
	// 2. 处于创建状态
	cond2 := eorm.C("status").EQ(storage.EventTypeCreated)
	// 3. 占有者主动放弃(续约时会检查是否需要放弃)，且候选者是当前storage
	cond3 := eorm.C("status").EQ(storage.EventTypeDiscarded).And(eorm.C("CandidateId").EQ(s.id))

	ctx, cancel := s.daoCtx()
	// TODO 这边应该有两个超时，一个是整体的超时，一个是单一数据库超时
	defer cancel()
	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(cond1.Or(cond2).Or(cond3)).
		GetMulti(ctx)

	if err != nil {
		log.Printf("ecron：[%s]本次抢占任务失败：%s", s.id, err)
		return
	}

	for _, item := range tasks {
		preemptedEvent := storage.Event{
			Type: storage.EventTypePreempted,
			Task: item,
		}

		// 更新前确保 task原来的占有者没有变化
		err = s.casTaskStatus(ctx, item.Id, item.Status, storage.EventTypePreempted)
		if err != nil {
			s.logger.Error("ecron：[%d] 抢占CAS操作错误：%s", s.id, err)
			continue
		}

		s.events <- preemptedEvent
	}
}

func (s *Storage) lookUpLoop() {
	// TODO 将轮询间隔做成可配置的
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			s.logger.Debug("ecron: storage %s 开始新一轮的抢占调度", s.id)
			// s.lookup()
			s.logger.Debug("ecron: storage %s 结束新一轮的抢占调度", s.id)
		case <-s.stop:
			s.logger.Debug("ecron: storage 退出抢占调度循环")
			return
		}
	}
}

// lookup 定时检查task的候选者是否需要更新当前storage
func (s *Storage) lookup() {
	var updateTaskIds []any
	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("status").EQ(storage.EventTypePreempted).
			And(eorm.C("OccupierId").NEQ(s.id))).GetMulti(ctx)
	if err != nil {
		log.Printf("ecron: [%d]更新候选者负载时，获取待更新任务时出错: %s", s.id, err)
	}

	var curTaskIndex int64 = 0
	for _, t := range tasks {
		curPayload := s.getPayload(ctx, s.id)

		occupierPayload := s.getPayload(ctx, t.OccupierId)
		// 获取要更新候选者为当前storage的task
		// 1. 如果task无候选者：
		//  -  比较task的占有者的负载(候选+占有)是不是比当前storage的负载大，
		//  -  同时还要保证大的数量是2*n，是的话就加入当前storage id到task的候选者中，并记录已经加入的个数，超过n就不在添加候选者
		// 2. 如果task有候选者()：
		//  - 比较方式同上，只不过比较的是候选者负载
		if t.CandidateId == "" {
			if occupierPayload > curPayload+2*s.n && curTaskIndex < s.n {
				updateTaskIds = append(updateTaskIds, t.Id)
				curTaskIndex += 1
				err = s.updateCandidateCAS(ctx, t.Id, t.OccupierId, t.CandidateId)
			}
		} else {
			candidatePayload := s.getPayload(ctx, t.CandidateId)
			if candidatePayload > curPayload+2*s.n && curTaskIndex < s.n {
				updateTaskIds = append(updateTaskIds, t.Id)
				curTaskIndex += 1
				err = s.updateCandidateCAS(ctx, t.Id, t.OccupierId, t.CandidateId)
			}
		}
		if err != nil {
			log.Printf("ecron: task[%d]更新从旧候选者[%d]更新候选者为[%d]出错: [%s]", t.Id,
				t.CandidateId, s.id, err)
		}
	}
}

// 计算指定storage节点的负载
// 包括作为候选者的负载、作为占有者的负载
// func (s *Storage) getPayload(ctx context.Context, id string) int64 {
// 	// 1. 作为候选者的负载
// 	cond1 := eorm.C("CandidateId").EQ(id)
// 	// 2. 作为占有者的负载, 注意这里需要去掉的该storage作为占有者的任务中已经有候选者的任务数
// 	cond2 := eorm.C("OccupierId").EQ(id).And(eorm.C("CandidateId").EQ(0))
// 	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
// 		Where(cond1.Or(cond2)).Get(ctx)
// 	if err != nil {
// 		return -1
// 	}
// 	return (*info).(int64)
// }

// func (s *Storage) updateCandidateCAS(ctx context.Context, taskId int64, occupierId, candidateId string) error {
// 	casConds := eorm.C("CandidateId").EQ(candidateId).And(eorm.C("OccupierId").EQ(occupierId))
// 	err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{CandidateId: s.id}).
// 		Set(eorm.C("CandidateId")).Where(eorm.C("Id").EQ(taskId).And(casConds)).Exec(ctx).Err()
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// Events 是一个幂等的方法。不论你调用多少次，最终效果都是一样的。
func (s *Storage) Events(taskEvents <-chan task.Event) (<-chan storage.Event, error) {
	s.startOnce.Do(func() {
		go s.handleTaskEvents(taskEvents)
		go s.lookUpLoop()
	})
	return s.events, nil
}

func (s *Storage) handleTaskEvents(taskEvents <-chan task.Event) {
	defer close(s.events)
	// for {
	// 	select {
	// 	case <-s.stop:
	// 		// 关闭事件
	// 		return
	// 	case event := <-taskEvents:
	// 		switch event.Type {
	// 		case task.EventTypeRunning:
	// 			// 插入一个运行记录
	// 			s.logger.Debug("任务 %s-%d 正在运行", event.Config().Name, event.Task.ID())
	// 		case task.EventTypeSuccess:
	// 			ctx, cancel := s.daoCtx()
	// 			err := s.casTaskStatus(ctx, event.Task.ID(), storage.EventTypePreempted,
	// 				storage.EventTypeEnd)
	// 			cancel()
	// 			if err != nil {
	// 				s.logger.Error("ecron: 未能更新任务  %s-%d 为成功状态, 原因：%v", event.Task.Config().Name, event.Task.ID(), err)
	// 				continue
	// 			}
	// 			s.logger.Debug("ecron: 更新任务  %s-%d  状态为成功", event.Config().Name, event.Task.ID())
	// 		case task.EventTypeFailed:
	// 			ctx, cancal := s.daoCtx()
	// 			err := s.casTaskStatus(ctx, event.Task.ID(), storage.EventTypePreempted,
	// 				storage.EventTypeEnd)
	// 			cancal()
	// 			if err != nil {
	// 				s.logger.Error("ecron: 未能更新任务  %s-%d 为成功状态, 原因：% ", event.Task.Config().Name, event.Task.ID(), err)
	// 				continue
	// 			}
	// 			s.logger.Debug("ecron: 更新任务 %s-%d 状态为失败", event.Config().Name, event.Task.ID())
	// 		default:
	// 			s.logger.Error("ecron: 未预期的事件类型 %s", event.Type)
	// 		}
	// 	}
	// }
}

// Refresh 获取所有已经抢占的任务，并更新时间和epoch
// 目前epoch仅作为续约持续时间的评估
func (s *Storage) refresh(ctx context.Context, task *TaskInfo, load int) {
	var (
		timer *time.Timer
		sc    []eorm.Assignable
	)
	sc = append(sc, eorm.Assign("Epoch", eorm.C("Epoch").Add(1)), eorm.C("UpdateTime"))
	for {
		s.logger.Debug("storage[%d] 开始续约task[%d]", s.id, task.Id)
		// 根据候选者负载决定是否需要放弃该任务，如果决定放弃就修改该任务状态为discard
		if task.CandidateId != "" && task.CandidateId {
			sc = append(sc, eorm.C("status"))
		}
		rowsAffect, err := eorm.NewUpdater[TaskInfo](s.db).
			Update(&TaskInfo{
				Status:     storage.EventTypeDiscarded,
				UpdateTime: time.Now().Unix(),
			}).Set(sc...).Where(eorm.C("Id").EQ(taskId).
			And(eorm.C("status").EQ(storage.EventTypePreempted)).
			And(eorm.C("OccupierId").EQ(s.id)).
			And(eorm.C("Epoch").EQ(epoch))).Exec(ctx).RowsAffected()

		// 1. 续约成功，退出该任务的续约，等待下一次到时间续约
		// 2. 获取的任务抢占状态改变，即已放弃了任务，终止这次续约，等待下一次到时间续约
		if err == nil {
			log.Printf("storage[%d]上task%d refresh success, 第%d次", s.id, taskId, epoch)
			return
		}
		if rowsAffect == 0 {
			log.Printf("storage[%d]上task%d refresh stop, 第%d次", s.id, taskId, epoch)
			return
		}

		// 这里意味续约失败，进行重试
		interval, ok := s.refreshRetry.Next()
		if !ok {
			log.Printf("storage[%d]上task%d refresh preempted fail, %s", s.id, taskId, err)
			return
		}

		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}
		select {
		case <-timer.C:
			log.Printf("ecron: storage[%d]续约开始第%d次重试", s.id, s.refreshRetry.GetCntRetry())
		case <-ctx.Done():
			log.Printf("ecron: storage[%d]续约终止，%s", s.id, ctx.Err())
			return
		}
	}
}

func (s *Storage) autoRefresh() {
	// 续约任务间隔
	timer := time.NewTicker(s.refreshInterval)
	tbl := eorm.TableOf(&TaskInfo{}, "t1")
	// 状态处于抢占中，并且抢占者恰好是自己
	ps := eorm.C("status").EQ(storage.EventTypePreempted).
		And(eorm.C("OccupierId").EQ(s.id))
	for {
		select {
		case <-timer.C:
			ctx, cancel := s.daoCtx()
			// 查询本 storage 已经抢占的所有任务，作为负载
			load, err := eorm.NewSelector[int](s.db).Select(eorm.Count("Id")).
				From(tbl).Where(ps).Get(ctx)
			if err != nil {
				s.logger.Error("ecron: 查询当前 Storage 的负载失败，err: %v", err)
				continue
			}
			cancel()

			ctx, cancel = s.daoCtx()
			// TODO 改成小批量逐步完成
			tasks, err := eorm.NewSelector[TaskInfo](s.db).
				From(eorm.TableOf(&TaskInfo{}, "t1")).
				Where(ps).GetMulti(ctx)
			cancel()
			if err != nil {
				s.logger.Error("ecron: 查找当前 storage %s 已经抢占的任务失败", s.id)
				continue
			}
			for _, t := range tasks {
				go s.refresh(ctx, t, *load)
			}
		case <-s.stop:
			log.Printf("ecron: storage[%s]关闭，停止所有task的自动续约", s.id)
			return
		}
	}
}

// 续约的时候决定当前storage是否要放弃一个任务，需要将候选者负载、占有者负载一起比较
func (s *Storage) isNeedDiscard(candidateId string) bool {
	return candidateId != ""
}

// 获取storage作为占有者的负载
// func (s *Storage) getOccupierPayload(ctx context.Context, id int64) int64 {
// 	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
// 		Where(eorm.C("OccupierId").EQ(id)).Get(ctx)
// 	if err != nil {
// 		return -1
// 	}
// 	return (*info).(int64)
// }

// 获取storage作为候选者的负载
// func (s *Storage) getCandidatePayload(ctx context.Context, id int64) int64 {
// 	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
// 		Where(eorm.C("CandidateId").EQ(id)).Get(ctx)
// 	if err != nil {
// 		return -1
// 	}
// 	return (*info).(int64)
// }

// func (s *Storage) delCandidate(ctx context.Context, id string, taskId int64) error {
// 	return eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
// 		UpdateTime: time.Now().Unix(),
// 	}).Set(eorm.Columns("CandidateId", "UpdateTime")).
// 		Where(eorm.C("Id").EQ(taskId).And(eorm.C("CandidateId").EQ(id))).Exec(ctx).Err()
// }

// Close storage的关闭, 这里终止所有正在执行的任务
func (s *Storage) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
	})
	return nil
}

func (s *Storage) daoCtx() (context.Context, func()) {
	// 改成可配置的。
	return context.WithTimeout(context.Background(), time.Second)
}
