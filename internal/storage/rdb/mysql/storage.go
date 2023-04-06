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

package mysql

import (
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/storage/rdb"
	"github.com/ecodeclub/eorm"
	"github.com/gotomicro/ekit/bean/option"
)

var _ storage.Storage = &Storage{}

func NewMysqlStorage(dsn string, opt ...option.Option[Storage]) (*Storage, error) {
	db, err := eorm.Open("mysql", dsn)
	if err != nil {
		return nil, errs.NewCreateStorageError(err)
	}
	return rdb.NewStorage(db, opt...)
}

type Storage = rdb.Storage
