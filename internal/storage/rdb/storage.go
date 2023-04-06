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
	"github.com/lithammer/shortuuid/v4"
	"log"
	"sync"
	"time"

	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/ecodeclub/eorm"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/ekit/bean/option"
)

type Storage struct {
	db                *eorm.DB
	preemptInterval   time.Duration         // 发起任务抢占时间间隔
	refreshInterval   time.Duration         // 发起任务抢占续约时间间隔
	preemptionTimeout time.Duration         // 抢占任务超时时间
	refreshRetry      storage.RetryStrategy // 续约重试策略
	occupierPayload   int64                 // 当前storage节点的占有者负载
	candidatePayload  int64                 // 当前storage节点的候选者负载
	id                string                // 唯一标识一个storage
	events            chan storage.Event
	stop              chan struct{}
	closeOnce         *sync.Once
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
		stop:      make(chan struct{}),
		closeOnce: &sync.Once{},
		n:         3,
		// 默认使用全局的日志，将来可以考虑允许用户覆盖
		logger: logger.Default(),
	}
	option.Apply[Storage](s, opt...)
	s.id = shortuuid.New()
	return s, nil
}

func WithPreemptInterval(t time.Duration) option.Option[Storage] {
	return func(s *Storage) {
		s.preemptInterval = t
	}
}

func WithRefreshRetry(r storage.RetryStrategy) option.Option[Storage] {
	return func(s *Storage) {
		s.refreshRetry = r
	}
}

func WithRefreshInterval(t time.Duration) option.Option[Storage] {
	return func(s *Storage) {
		s.refreshInterval = t
	}
}

func WithPreemptTimeout(t time.Duration) option.Option[Storage] {
	return func(s *Storage) {
		s.preemptionTimeout = t
	}
}

// Add 创建task，设置调度状态是created
func (s *Storage) Add(ctx context.Context, t task.Task) (int64, error) {
	cfg := t.Config()
	id, err := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
		Name:            cfg.Name,
		Cron:            cfg.Cron,
		SchedulerStatus: storage.EventTypeCreated,
		Type:            cfg.Type,
		Parameters:      cfg.Parameters,
		CreateTime:      time.Now().Unix(),
		UpdateTime:      time.Now().Unix(),
	}).Exec(ctx).LastInsertId()
	if err != nil {
		return -1, errs.NewAddTaskError(err)
	}
	return id, nil
}

// AddExecution 创建一条执行记录
//func (s *Storage) AddExecution(ctx context.Context, taskId int64) (int64, error) {
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
//}

func (s *Storage) compareAndUpdateTaskStatus(ctx context.Context, taskId int64, old, new string) error {
	cond := eorm.C("Id").EQ(taskId).And(eorm.C("SchedulerStatus").EQ(old))
	ra, err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		SchedulerStatus: new,
		OccupierId:      s.id,
		UpdateTime:      time.Now().Unix(),
	}).Set(eorm.Columns("SchedulerStatus", "UpdateTime", "OccupierId")).Where(cond).Exec(ctx).RowsAffected()
	if err != nil {
		return errs.NewCompareAndUpdateDbError(err)
	}
	if ra == 0 {
		return errs.NewCompareAndUpdateAffectZeroError()
	}
	return nil
}

func (s *Storage) compareAndUpdateTaskExecutionStatus(ctx context.Context, taskId int64, old, new string) error {
	cond := eorm.C("TaskId").EQ(taskId).And(eorm.C("ExecuteStatus").EQ(old))
	ra, err := eorm.NewUpdater[TaskExecution](s.db).Update(&TaskExecution{
		ExecuteStatus: new,
		UpdateTime:    time.Now().Unix(),
	}).Set(eorm.Columns("ExecuteStatus", "UpdateTime")).Where(cond).Exec(ctx).RowsAffected()
	if err != nil {
		return errs.NewCompareAndUpdateDbError(err)
	}
	if ra == 0 {
		return errs.NewCompareAndUpdateAffectZeroError()
	}
	return nil
}

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

// RunPreempt 每隔固定时间去db中抢占任务
func (s *Storage) RunPreempt() {
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
	maxRefreshInterval := s.refreshRetry.GetMaxRetry() * int64(s.refreshInterval.Seconds())
	// 1. 长时间没有续约的已经抢占的任务
	cond1 := eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
		And(eorm.C("UpdateTime").LTEQ(time.Now().Unix() - maxRefreshInterval))
	// 2. 处于创建状态
	cond2 := eorm.C("SchedulerStatus").EQ(storage.EventTypeCreated)
	// 3. 占有者主动放弃(续约时会检查是否需要放弃)，且候选者是当前storage
	cond3 := eorm.C("SchedulerStatus").EQ(storage.EventTypeDiscarded).And(eorm.C("CandidateId").EQ(s.id))

	ctx, cancel := s.daoCtx()
	// TODO 这边应该有两个超时，一个是整体的超时，一个是单一数据库超时
	defer cancel()
	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(cond1.Or(cond2).Or(cond3)).
		GetMulti(ctx)

	if err != nil {
		log.Printf("ecron：[%d]本次抢占任务数：%s", s.id, err)
		return
	}

	for _, item := range tasks {
		preemptedEvent := storage.Event{
			Type: storage.EventTypePreempted,
			Task: item,
		}

		// 更新前确保 task原来的占有者没有变化
		err = s.compareAndUpdateTaskStatus(ctx, item.Id, item.SchedulerStatus, storage.EventTypePreempted)
		if err != nil {
			log.Printf("ecron：[%d]抢占CAS操作错误：%s", s.id, err)
			continue
		}

		// 如果要删除候选者，需要更新db中候选者负载，并同步负载到候选者内存...
		// 考虑不同storage间的负载同步问题，主要是候选者负载同步问题，不在通过内存保存负载
		if item.CandidateId != "" {
			err = s.delCandidate(ctx, item.CandidateId, item.Id)
			if err != nil {
				log.Printf("ecron: [%d]删除task[%d]候选者storage[%d]时出现错误: %v",
					s.id, item.Id, item.CandidateId, err)
				continue
			}
		}
		s.events <- preemptedEvent
	}
}

func (s *Storage) AutoLookup(ctx context.Context) {
	timer := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-timer.C:
			log.Printf("ecron: storge[%d]开始候选者标记", s.id)
			s.lookup(ctx)
		case <-s.stop:
			log.Printf("ecron: storage[%d]关闭，停止所有task的负载均衡", s.id)
			return
		}
	}
}

// 定时检查task的候选者是否需要更新当前storage
func (s *Storage) lookup(ctx context.Context) {
	var updateTaskIds []any
	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
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
func (s *Storage) getPayload(ctx context.Context, id string) int64 {
	// 1. 作为候选者的负载
	cond1 := eorm.C("CandidateId").EQ(id)
	// 2. 作为占有者的负载, 注意这里需要去掉的该storage作为占有者的任务中已经有候选者的任务数
	cond2 := eorm.C("OccupierId").EQ(id).And(eorm.C("CandidateId").EQ(0))
	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(cond1.Or(cond2)).Get(ctx)
	if err != nil {
		return -1
	}
	return (*info).(int64)
}

func (s *Storage) updateCandidateCAS(ctx context.Context, taskId int64, occupierId, candidateId string) error {
	casConds := eorm.C("CandidateId").EQ(candidateId).And(eorm.C("OccupierId").EQ(occupierId))
	err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{CandidateId: s.id}).
		Set(eorm.C("CandidateId")).Where(eorm.C("Id").EQ(taskId).And(casConds)).Exec(ctx).Err()
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) Events(taskEvents <-chan task.Event) (<-chan storage.Event, error) {
	go func() {
		for {
			select {
			case <-s.stop:
				close(s.events)
				return
			case event := <-taskEvents:
				switch event.Type {
				case task.EventTypeRunning:
					s.logger.Debug("任务 %s-%d 正在运行", event.Config().Name, event.Task.ID())
				case task.EventTypeSuccess:
					ctx, cancel := s.daoCtx()
					err := s.compareAndUpdateTaskStatus(ctx, event.Task.ID(), storage.EventTypePreempted,
						storage.EventTypeEnd)
					cancel()
					if err != nil {
						s.logger.Error("ecron: 未能更新任务  %s-%d 为成功状态, 原因：%v", event.Task.Config().Name, event.Task.ID(), err)
						continue
					}
					s.logger.Debug("ecron: 更新任务  %s-%d  状态为成功", event.Config().Name, event.Task.ID())
				case task.EventTypeFailed:
					ctx, cancal := s.daoCtx()
					err := s.compareAndUpdateTaskStatus(ctx, event.Task.ID(), storage.EventTypePreempted,
						storage.EventTypeEnd)
					cancal()
					if err != nil {
						s.logger.Error("ecron: 未能更新任务  %s-%d 为成功状态, 原因：% ", event.Task.Config().Name, event.Task.ID(), err)
						continue
					}
					s.logger.Debug("ecron: 更新任务 %s-%d 状态为失败", event.Config().Name, event.Task.ID())
				}
			}
		}
	}()
	return s.events, nil
}

// Refresh 获取所有已经抢占的任务，并更新时间和epoch
// 目前epoch仅作为续约持续时间的评估
func (s *Storage) refresh(ctx context.Context, taskId, epoch int64, candidateId string) {
	var (
		timer *time.Timer
		sc    []eorm.Assignable
	)
	sc = append(sc, eorm.Assign("Epoch", eorm.C("Epoch").Add(1)), eorm.C("UpdateTime"))
	for {
		log.Printf("storage[%d] 开始续约task[%d]", s.id, taskId)
		// 根据候选者负载决定是否需要放弃该任务，如果决定放弃就修改该任务状态为discard
		needDiscard := s.isNeedDiscard(ctx, candidateId)
		if needDiscard {
			sc = append(sc, eorm.C("SchedulerStatus"))
		}
		rowsAffect, err := eorm.NewUpdater[TaskInfo](s.db).
			Update(&TaskInfo{
				SchedulerStatus: storage.EventTypeDiscarded,
				UpdateTime:      time.Now().Unix(),
			}).Set(sc...).Where(eorm.C("Id").EQ(taskId).
			And(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted)).
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

func (s *Storage) AutoRefresh(ctx context.Context) {
	// 续约任务间隔
	timer := time.NewTicker(s.refreshInterval)
	for {
		select {
		case <-timer.C:
			tasks, _ := eorm.NewSelector[TaskInfo](s.db).
				From(eorm.TableOf(&TaskInfo{}, "t1")).
				Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
					And(eorm.C("OccupierId").EQ(s.id))).
				GetMulti(ctx)
			for _, t := range tasks {
				go s.refresh(ctx, t.Id, t.Epoch, t.CandidateId)
			}
		case <-s.stop:
			log.Printf("ecron: storage[%d]关闭，停止所有task的自动续约", s.id)
			return
		}
	}
}

// 续约的时候决定当前storage是否要放弃一个任务，需要将候选者负载、占有者负载一起比较
func (s *Storage) isNeedDiscard(ctx context.Context, candidateId string) bool {
	return candidateId != ""
}

// 获取storage作为占有者的负载
func (s *Storage) getOccupierPayload(ctx context.Context, id int64) int64 {
	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("OccupierId").EQ(id)).Get(ctx)
	if err != nil {
		return -1
	}
	return (*info).(int64)
}

// 获取storage作为候选者的负载
func (s *Storage) getCandidatePayload(ctx context.Context, id int64) int64 {
	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("CandidateId").EQ(id)).Get(ctx)
	if err != nil {
		return -1
	}
	return (*info).(int64)
}

func (s *Storage) delCandidate(ctx context.Context, id string, taskId int64) error {
	return eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		UpdateTime: time.Now().Unix(),
	}).Set(eorm.Columns("CandidateId", "UpdateTime")).
		Where(eorm.C("Id").EQ(taskId).And(eorm.C("CandidateId").EQ(id))).Exec(ctx).Err()
}

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
