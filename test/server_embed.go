//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package test

import (
	"fmt"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"time"
)

// Example of how to implement a MySQL server based on a Engine:
//
// ```
// > mysql --host=127.0.0.1 --port=3306 -u root mydb -e "SELECT * FROM mytable"
// +----------+-------------------+-------------------------------+---------------------+
// | name     | email             | phone_numbers                 | created_at          |
// +----------+-------------------+-------------------------------+---------------------+
// | John Doe | john@doe.com      | ["555-555-555"]               | 2018-04-18 09:41:13 |
// | John Doe | johnalt@doe.com   | []                            | 2018-04-18 09:41:13 |
// | Jane Doe | jane@doe.com      | []                            | 2018-04-18 09:41:13 |
// | Evil Bob | evilbob@gmail.com | ["555-666-555","666-666-666"] | 2018-04-18 09:41:13 |
// +----------+-------------------+-------------------------------+---------------------+
// ```
func createEmbedDatabase() {
	engine := sqle.NewDefault(
		sql.NewDatabaseProvider(
			createTestDatabase(),
			information_schema.NewInformationSchemaDatabase(),
		))

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3307",
		Auth:     auth.NewNativeSingle("root", "123456", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}

	s.Start()
}

func createTestDatabase() *memory.Database {
	const (
		dbName    = "employees"
		tableNamePrefix = "student"
	)

	db := memory.NewDatabase(dbName)
	for i := 0; i < 32; i++ {
		tableName := fmt.Sprintf("%s_%v", tableNamePrefix, fmt.Sprintf("%04d", i))
		score, _ := sql.NewColumnDefaultValue(expression.NewLiteral(float64(0), sql.Float64), nil, true, true)
		birth, _ := sql.NewColumnDefaultValue(expression.NewLiteral(uint16(0), sql.Uint16), nil, true, true)
		created, _ := sql.NewColumnDefaultValue(expression.NewLiteral(time.Now(), sql.Timestamp), nil, true, true)
		modified, _ := sql.NewColumnDefaultValue(expression.NewLiteral(time.Now(), sql.Timestamp), nil, true, true)
		table := memory.NewTable(tableName, sql.Schema{
			{Name: "id", Type: sql.Uint64, Nullable: false, Source: tableName, PrimaryKey: true, AutoIncrement: true},
			{Name: "uid", Type: sql.Uint64, Nullable: false, Source: tableName},
			{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
			{Name: "score", Type: sql.Float64, Nullable: true, Source: tableName, Default: score},
			{Name: "nickname", Type: sql.Text, Nullable: true, Source: tableName},
			{Name: "gender", Type: sql.Uint8, Nullable: true, Source: tableName},
			{Name: "birth_year", Type: sql.Uint16, Nullable: true, Source: tableName, Default: birth},
			{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName, Default: created},
			{Name: "modified_at", Type: sql.Timestamp, Nullable: false, Source: tableName, Default: modified},
		})
		ctx := sql.NewEmptyContext()
		table.Insert(ctx, sql.NewRow(i, i, "John Doe", 0, "John", 1, 12, time.Now(), time.Now()))
		table.Insert(ctx, sql.NewRow(i + 1, i + 1, "John Doe", 0, "John", 1, 12, time.Now(), time.Now()))
		db.AddTable(tableName, table)
	}
	return db
}
