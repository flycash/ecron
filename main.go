package main

import (
	"context"
	"github.com/ecodeclub/ecron/internal/storage/rdb"
	"time"

	"github.com/ecodeclub/ecron/internal/scheduler"
)

func main() {
	ctx := context.TODO()
	storeIns, err := rdb.NewMysqlStorage("root:@tcp(localhost:3306)/ecron",
		rdb.WithPreemptInterval(1*time.Second))
	if err != nil {
		return
	}
	go storeIns.RunPreempt(ctx)
	go storeIns.AutoRefresh(ctx)

	sche := scheduler.NewScheduler(storeIns)
	go sche.Start(ctx)

	select {}
}
