// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

//#include <stdio.h>
//#include <stdlib.h>
//#include "sqlite3.h"
import "C"
import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

func (d *DB) ExportDataTableToCSV(dt DataTable, separator ...string) []byte {

	if len(dt.Rows) == 0 {
		return nil
	}

	var allLines []string
	var headerStr []string
	sep := `"`

	if len(separator) > 0 {
		sep = separator[0]
	}

	for i := 0; i < len(dt.Columns); i++ {
		headerStr = append(headerStr, fmt.Sprintf(`"%s"`, dt.Columns[i].Name))
	}

	// First line is the header columns.
	allLines = append(allLines, strings.Join(headerStr, ","))

	for i := 0; i < len(dt.Rows); i++ {
		var line []string

		for j := 0; j < len(dt.Columns); j++ {
			fldVal := fmt.Sprintf("%v", dt.Rows[i][dt.Columns[j].Name])
			ecp := fmt.Sprintf(`%s%s`, sep, sep)
			fldVal = strings.ReplaceAll(fldVal, sep, ecp)
			line = append(line, fmt.Sprintf(`%s%v%s`, sep, fldVal, sep))
		}
		allLines = append(allLines, strings.Join(line, ","))
	}

	return []byte(strings.Join(allLines, "\r\n"))
}

func (d *DB) DataTableToJSON(dt DataTable) string {

	if len(dt.Rows) == 0 {
		return ""
	}

	var wrkRes string
	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan string)
	go func() {
		var jsnArry []string
		for i := 0; i < len(dt.Rows); i++ {
			var sa []string
			for j := 0; j < len(dt.Columns); j++ {
				v := dt.Rows[i][dt.Columns[j].Name]
				if fmt.Sprintf("%T", dt.Rows[i][dt.Columns[j].Name]) == "string" {
					s := fmt.Sprintf("%v", v)
					if !strings.HasPrefix(s, "{") {
						v = fmt.Sprintf(`"%v"`, s)
					}
				}
				sa = append(sa, fmt.Sprintf(`"%s":%v`, dt.Columns[j].Name, v))
			}
			oneJsn := fmt.Sprintf(`{%s}`, strings.Join(sa, ","))

			jsnArry = append(jsnArry, oneJsn)
		}
		c <- fmt.Sprintf(`[%s]`, strings.Join(jsnArry, ","))
	}()

	wrkRes = <-c
	close(c)

	return wrkRes
}

func (dt *DataTable) ExportToCSV(destFilePath string) error {

	if len(dt.Rows) == 0 {
		return errors.New("data-table is empty")
	}

	if fileOrDirExists(destFilePath) {
		return errors.New("destination file already exists")
	}

	var allLines []string
	var headerStr []string

	for i := 0; i < len(dt.Columns); i++ {
		headerStr = append(headerStr, fmt.Sprintf(`"%s"`, dt.Columns[i].Name))
	}

	// First line is the header columns.
	allLines = append(allLines, strings.Join(headerStr, ","))

	for i := 0; i < len(dt.Rows); i++ {
		var line []string
		for j := 0; j < len(dt.Columns); j++ {
			oneValue := fmt.Sprintf("%v", dt.Rows[i][dt.Columns[j].Name])
			oneValue = strings.TrimSpace(oneValue)
			oneValue = strings.ReplaceAll(oneValue, "<nil>", "")
			line = append(line, fmt.Sprintf(`"%s"`, oneValue))
		}

		allLines = append(allLines, strings.Join(line, ","))
	}

	bToWriteToDisk := []byte(strings.Join(allLines, "\r\n"))

	err := os.WriteFile(destFilePath, bToWriteToDisk, os.ModePerm)

	return err
}

// Update updates rows/cols of a table as they appear in the DataTable.
// The table and its rows must exist in a single table and not resulted
// from multiple joins to other tables and/or include aliases.
func (dt *DataTable) Update() Result {

	var res Result
	b := strings.Builder{}

	cols := dt.db.getTableColumns(dt.Name)
	primarKeyColName := ""
	var primarKeyValue any
	for i := 0; i < len(cols); i++ {
		if cols[i].IsPrimaryKey {
			primarKeyColName = cols[i].Name
		}
	}

	// TODO:
	// ** check the table name; must eixts; no aliases
	// ** there must be a primary key column
	// ** include the rowid in GetDataTable() as a build-in option?
	// ** warn the caller if the table has been created without rowid.
	for i := 0; i < len(dt.Rows); i++ {
		b.Reset()
		for j := 0; j < len(dt.Columns); j++ {
			if j == 0 {
				b.WriteString(fmt.Sprintf("update [%s] set ", dt.Name))
			}
			colName := dt.Columns[j].Name

			// skip col if not found in the orginal column
			// as it could be an alias
			colFound := false
			for k := 0; k < len(cols); k++ {
				colsNameLower := strings.ToLower(colName)
				if strings.ToLower(cols[k].Name) == colsNameLower ||
					strings.ToLower(primarKeyColName) == colsNameLower {
					colFound = true
					break
				}
			}
			if !colFound {
				continue
			}
			valStr := ""
			tv := dt.Rows[i][colName]

			// TODO: composite key
			if primarKeyValue == nil && primarKeyColName == dt.Columns[j].Name {
				primarKeyValue = tv
			}

			// this may not necessarily be of the column type; it is to determin
			// wether the value has to be single-quoted.
			t := fmt.Sprintf("%T", dt.Rows[i][dt.Columns[j].Name])
			switch t {
			case "time.Time":
				// there is no date type in sqlite; only string
				valStr = fmt.Sprintf("'%s'", tv.(string))

			case "string":
				valStr = fmt.Sprintf("'%s'", tv.(string))

			case "[]uint8": /*blob*/
				valStr = fmt.Sprintf("x'%x'", tv.([]byte))

			case "int":
				valStr = fmt.Sprintf("%d", tv.(int))

			case "int64":
				valStr = fmt.Sprintf("%d", tv.(int64))

			case "<nil>":
				valStr = "NULL"
			}

			if dt.Columns[j].Name != primarKeyColName {
				b.WriteString(fmt.Sprintf("[%s] = %s, ", dt.Columns[j].Name, valStr))
			}
		}

		if primarKeyValue == nil {
			res.err = errors.New("table has no primary key")
			return res
		}

		sqlx := strings.TrimSpace(b.String())
		sqlx = strings.TrimSuffix(sqlx, ",")

		primarKeySQL := ""
		t := fmt.Sprintf("%T", primarKeyValue)
		if t == "string" || t == "time.Time" {
			primarKeySQL = fmt.Sprintf("%s='%s'", primarKeyColName, primarKeyValue)

		} else if t == "int" || t == "int64" || t == "float64" {
			primarKeySQL = fmt.Sprintf("%s=%d", primarKeyColName, primarKeyValue)

		} else if t == "[]int8" {
			// blob; as byte format
			primarKeySQL = fmt.Sprintf("%s=x'%s'", primarKeyColName, primarKeyValue)
		}
		sqlx = fmt.Sprintf("%s where %s", sqlx, primarKeySQL)
		res = dt.db.Exec(sqlx)
		if res.err != nil {
			return res
		}
		if res.rowsAffected < 1 {
			res.err = errors.New("no work done")
			return res
		}
	}

	return res
}

func (d *DB) GetDataTableAsync(callback func(*DataTable), query string, placeHolders ...any) (*DataTable, error) {

	var wrkRes DataTable

	if d.Closed {
		wrkRes.Err = errors.New("database is not open")
		return &wrkRes, wrkRes.Err
	}

	GetDataTableSeqNo++

	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan DataTable)
	go func() {
		var wrk DataTable
		wrk.db = d
		wrk.SeqNo = GetDataTableSeqNo
		wrk.TimeStarted = time.Now()

		s, _, err := d.Prepare(query, placeHolders)
		if err == nil {
			wrk.Name = getTableNameFromSQLQuery(query)
			colCnt := int(C.sqlite3_column_count(s.cStmt))
			for i := 0; i < colCnt; i++ {
				// a column name from the query (cStmt) could be
				// an alias, which would not be in the schema.
				colName := C.GoString(C.sqlite3_column_name(s.cStmt, C.int(i)))
				wrk.Columns = append(wrk.Columns, Column{
					Name: colName, Ordinal: i})
			}

			colDesc := d.getTableColumns(wrk.Name)

			// set the columns first.
			for i := 0; i < len(wrk.Columns); i++ {
				for k := 0; k < len(colDesc); k++ {
					if colDesc[k].Name == wrk.Columns[i].Name {
						wrk.Columns[i] = colDesc[k]
						break
					}
				}
				// ordinal is relative to the columns
				// in the query; replace it.
				wrk.Columns[i].Ordinal = i
			}
			// fetch rows
			for {
				rc := C.sqlite3_step(s.cStmt)
				if rc != SQLITE_ROW {
					break
				}
				m := make(map[string]any, 1)
				for i := 0; i < len(wrk.Columns); i++ {
					m[wrk.Columns[i].Name] = d.getStmtColVal(&s, i)

					// If the DataType is empty, the column-name
					// must be an alias (e.g. _rowid_ as RowID);
					// get the data-type from the value.
					if i == 0 && wrk.Columns[i].DataType == "" {
						t := fmt.Sprintf("%T", m[wrk.Columns[i].Name])
						switch t {
						case "string":
							wrk.Columns[i].DataType = "TEXT"

						case "int64":
							wrk.Columns[i].DataType = "INTEGER"

						case "[]uint8":
							wrk.Columns[i].DataType = "BLOB"

						case "float64":
							wrk.Columns[i].DataType = "REAL"
						}
					}
				}
				wrk.Rows = append(wrk.Rows, m)
			}
			C.sqlite3_finalize(s.cStmt)
		}
		c <- wrk
	}()
	wrkRes = <-c
	close(c)

	wrkRes.TimeEnded = time.Now()

	GetDataTableSeqNo--

	if callback != nil {
		callback(&wrkRes)
	}

	return &wrkRes, wrkRes.Err
}

func (d *DB) GetDataTable(query string, placeHolders ...any) (*DataTable, error) {

	var wrkRes DataTable

	query = strings.TrimSpace(query)
	if len(query) < 7 {
		wrkRes.Err = errors.New("invalid query")
		return &wrkRes, wrkRes.Err
	}

	if !strings.EqualFold(query[:6], "SELECT") && !strings.EqualFold(query[:6], "PRAGMA") {
		wrkRes.Err = errors.New("invalid query")
		return &wrkRes, wrkRes.Err
	}

	if d == nil || d.Closed {
		wrkRes.Err = errors.New("database is not open")
		return &wrkRes, wrkRes.Err
	}

	if !d.isInMemory {
		_, err := os.Stat(d.filePath)
		if os.IsNotExist(err) {
			wrkRes.Err = errors.New("database is not open")
			return &wrkRes, wrkRes.Err
		}
	}

	GetDataTableSeqNo++

	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan DataTable)
	go func() {
		var wrk DataTable
		wrk.db = d
		wrk.SeqNo = GetDataTableSeqNo
		wrk.TimeStarted = time.Now()
		s, pzTail, err := d.Prepare(query, placeHolders)
		if err == nil {
			wrk.SQLTail = pzTail
			wrk.Name = getTableNameFromSQLQuery(query)
			colCnt := int(C.sqlite3_column_count(s.cStmt))
			for i := 0; i < colCnt; i++ {
				// a column name from the query (cStmt) could be
				// an alias, which would not be in the schema.
				colName := C.GoString(C.sqlite3_column_name(s.cStmt, C.int(i)))
				wrk.Columns = append(wrk.Columns, Column{
					Name: colName, Ordinal: i})
			}

			colDesc := d.getTableColumns(wrk.Name)

			// set the columns first; dynamic + IsPrimaryKey
			for i := 0; i < len(wrk.Columns); i++ {
				for k := 0; k < len(colDesc); k++ {
					if colDesc[k].Name == wrk.Columns[i].Name {
						wrk.Columns[i] = colDesc[k]
						break
					}
				}
				// ordinal is relative to the columns
				// in the query; replace it.
				wrk.Columns[i].Ordinal = i
			}
			// fetch the rows
			hitTest := false
			for {
				rc := C.sqlite3_step(s.cStmt)
				if rc != SQLITE_ROW {
					break
				}

				hitTest = true
				m := make(map[string]any, 1)

				for i := 0; i < len(wrk.Columns); i++ {
					m[wrk.Columns[i].Name] = d.getStmtColVal(&s, i)

					// If the DataType is empty, the column-name
					// must be an alias (e.g. _rowid_ as RowID);
					// get the data-type from the value.
					// Reset the date-type of the column:
					if i == 0 {
						wrk.Columns[i].DataType = GetSQLiteDataType(m[wrk.Columns[i].Name])
					}
				}
				wrk.Rows = append(wrk.Rows, m)
			}

			if !hitTest {
				wrk.Columns = nil
			}
			C.sqlite3_finalize(s.cStmt)
		} else {
			wrk.Err = err
		}
		c <- wrk
	}()
	wrkRes = <-c
	close(c)

	wrkRes.TimeEnded = time.Now()

	GetDataTableSeqNo--

	return &wrkRes, wrkRes.Err
}

func (dt *DataTable) DescribeDatabase() DBStat {
	return dt.db.Describe()
}
