// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

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

// CreateTable creates a table in-memory, regardless
// of how the its database was opened; as a database opened
// via a file can still create in-memory tables.
func (i *InMemoryObjects) CreateTable(sqlx string) (Table, error) {

	var t Table
	var cols []Column

	sqlx = strings.TrimSpace(sqlx)
	if !strings.HasPrefix(sqlx, ";") {
		sqlx = sqlx + ";"
	}
	sqlxx := removeDoubleSpace(sqlx)
	sqlxx = strings.ToUpper(sqlxx)

	if !strings.HasPrefix(sqlxx, "CREATE TEMP TABLE") {
		return t, errors.New("expected sql statement to begin with: CREATE TEMP TABLE")
	}

	v := strings.Split(sqlxx, "(")
	t.Name = strings.TrimSpace(strings.ReplaceAll(v[0], "CREATE TEMP TABLE ", ""))
	firstCol := strings.Split(v[1], ",")[0]
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
		c.Name = v2[0]
		c.DataType = v2[1]
		cols = append(cols, c)
	}
	t.Columns = cols

	// Create in-memory temp table for variable.
	// 2 means use in-memory:
	// PRAGMA temp_store = 0 | DEFAULT | 1 | FILE | 2 | MEMORY;
	_, err := i.db.ExecuteNonQuery("PRAGMA temp_store = 2;")
	if err != nil {
		return t, err
	}
	_, err = i.db.ExecuteNonQuery(sqlx)
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
