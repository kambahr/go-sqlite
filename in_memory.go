// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

package gosqlite

import (
	"errors"
	"fmt"
	"strings"
)

// InMemoryObjects holds a pointer to a database
// and array of tables. Note that a database opened via
// file can create in-memory tables.
type InMemoryObjects struct {
	Tables []Table
	db     *DB
}

func (i *InMemoryObjects) formatSQL(s string) string {

	for range 5 {
		s = strings.ReplaceAll(s, "\n", " ")
		s = strings.ReplaceAll(s, "\t", " ")
		s = strings.ReplaceAll(s, ", ", ",")
		s = strings.ReplaceAll(s, "( ", "( ")
		s = strings.ReplaceAll(s, ") ", ") ")
		s = removeDoubleSpace(s)

		s = strings.TrimSpace(s)
		if !strings.HasPrefix(s, ";") {
			s = s + ";"
		}
		s = strings.ReplaceAll(s, ";;", ";")
	}

	return s
}

// CreateTable creates a table in-memory, regardless
// of how the its database was opened; as a database opened
// via a file can still create in-memory tables.
func (i *InMemoryObjects) CreateTable(sqlx string) (Table, error) {

	var t Table
	var cols []Column

	sqlxx := i.formatSQL(sqlx)
	sqlxx = RemoveCommentsFromString(sqlxx, "/*", "*/")

	v := strings.Split(sqlxx, "(")
	t.Name = strings.TrimSpace(strings.ReplaceAll(v[0], "CREATE TEMP TABLE ", ""))
	t.Name = strings.TrimSpace(strings.ReplaceAll(v[0], "CREATE TEMPORARY TABLE ", ""))
	firstCol := strings.TrimSpace(strings.Split(v[1], ",")[0])
	var c Column
	v2 := strings.Split(firstCol, " ")
	c.Name = v2[0]
	c.DataType = v2[1]
	if strings.Contains(strings.ToUpper(firstCol), "PRIMARY KEY") {
		c.IsPrimaryKey = true
	}
	cols = append(cols, c)

	v = strings.Split(sqlxx, ",")
	for i := 1; i < len(v); i++ {
		var c Column
		s := strings.TrimSpace(v[i])
		s = strings.Trim(s, ";")
		s = strings.Trim(s, ")")
		if strings.Contains(strings.ToUpper(firstCol), "PRIMARY KEY") {
			c.IsPrimaryKey = true
		}
		v2 := strings.Split(s, " ")
		if len(v2) < 2 {
			return Table{}, errors.New("malformed sql statement")
		}
		c.Name = v2[0]
		c.DataType = v2[1]
		cols = append(cols, c)
	}
	t.Columns = cols

	// 2 means use in-memory:
	// PRAGMA temp_store = 0 | DEFAULT | 1 | FILE | 2 | MEMORY;
	_, err := i.db.ExecuteNonQuery("PRAGMA temp_store = 2;")
	if err != nil {
		return t, err
	}

	_, err = i.db.ExecuteNonQuery(sqlxx)
	if err != nil {
		return t, err
	}

	sqlz := fmt.Sprintf("SELECT sql FROM sqlite_temp_master WHERE type = 'table' AND UPPER(tbl_name) = '%s' LIMIT 1", strings.ToUpper(t.Name))
	dt, err := i.db.GetDataTable(sqlz)
	if err != nil {
		return t, err
	}
	if len(dt.Rows) == 0 {
		return t, errors.New("failed to create temp table")
	}
	t.CreateSQL = dt.Rows[0]["sql"].(string)

	i.Tables = append(i.Tables, t)

	return t, err
}
