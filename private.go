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
	"path"
	"strings"
	"time"
	"unsafe"
)

// initDB creates a new instance of DB.
// If dbFilePath is emtpy an in-memory DB
// is assumed.
func initDB(dbFilePath string) DB {

	var inMem InMemoryObjects

	var db = DB{
		DBHwnd:     nil,
		filePath:   dbFilePath,
		UniqueName: createHash(dbFilePath),
		isInMemory: len(dbFilePath) == 0,
		ConnPool: ConnectionPool{
			MaxOpenConns:          0, /* 0 means unlimited */
			MaxIdleConns:          0, /* 0 means unlimited */
			MaxLifetime:           0, /* 0 means unlimited */
			MaxIdleTime:           0, /* 0 means unlimited */
			MaxConcurrentRequests: 100000},
		Closed:                  true,
		TimeOpened:              time.Now(),
		ConnString:              fmt.Sprintf("file_path=%s", dbFilePath),
		Name:                    strings.TrimSuffix(path.Base(dbFilePath), ".sqlite"), /* default */
		getResultSetConnections: 0,
	}

	db.intfce = &db
	inMem.db = &db
	db.InMemory = inMem

	return db
}

// fixPragmaTextAndOrder edits the pragma entries:
// smicolon at the end, wall journal_mode to accompany
// with checkpoint and also some formatting mistakes.
func fixPragmaTextAndOrder(pragArry []string) []string {

	if len(pragArry) == 0 {
		return pragArry
	}
	//Fix the PRAGMA text
	for i := 0; i < len(pragArry); i++ {
		pragArry[i] = strings.Trim(pragArry[i], " ")
		// take out extra spaces
		for {
			if !strings.Contains(pragArry[i], "  ") {
				break
			}
			pragArry[i] = strings.ReplaceAll(pragArry[i], "  ", " ")
		}
		pragArry[i] = strings.ToLower(pragArry[i])
		if !strings.HasSuffix(pragArry[i], ";") {
			pragArry[i] = fmt.Sprintf("%s;", pragArry[i])
		}
		if !strings.Contains(pragArry[i], "pragma ") {
			pragArry[i] = strings.ReplaceAll(pragArry[i], "pragma", "pragma ")
		}
	}
	// if set to wal, make sure pragma wal_checkpoint(passive) is included
	wallExists := false
	for i := 0; i < len(pragArry); i++ {
		if strings.Contains(pragArry[i], "journal_mode = wal") || strings.Contains(pragArry[i], "journal_mode=wal") {
			wallExists = true
			break
		}
	}
	if wallExists {
		// the wall and the checkpoint pragma statements
		// have to be in consecutive order.
		var s []string
		s = append(s, "pragma journal_mode = wal;")
		s = append(s, "pragma wal_checkpoint(passive);")

		for i := 0; i < len(pragArry); i++ {
			if !strings.Contains(pragArry[i], "wal_checkpoint") &&
				!(strings.Contains(pragArry[i], "journal_mode = wal") || strings.Contains(pragArry[i], "journal_mode=wal")) {
				s = append(s, pragArry[i])
				break
			}
		}
		pragArry = s
	}

	return pragArry
}

// removeFromResultQueue removes a query request
// from the global mResultQueue.
func (d *DB) removeFromResultQueue(queryID string) {

	if len(mResultQueue) == 0 {
		return
	}
	mCMutex.Lock()
	for i := 0; i < len(mResultQueue); i++ {
		if mResultQueue[i].QueryID == queryID {
			mResultQueue[len(mResultQueue)-1], mResultQueue[i] = mResultQueue[i], mResultQueue[len(mResultQueue)-1]
			mResultQueue = mResultQueue[:len(mResultQueue)-1]
			break
		}
	}
	mCMutex.Unlock()
}

// decrementActiveConn derements getResultSetConnections
func (d *DB) decrementActiveConn() {
	d.getResultSetConnections--
}

// getIndexColumns parses a list of columns that
// are indexed from a create-table sql statement.
func (d *DB) getIndexColumns(creatTableSQL string) []string {

	v := strings.Split(creatTableSQL, " ")
	s := v[len(v)-1]
	s = strings.TrimSuffix(s, ")")
	s = strings.TrimPrefix(s, "(")

	sa := strings.Split(s, ",")

	for i := 0; i < len(sa); i++ {
		sa[i] = strings.TrimSpace(sa[i])

		sa[i] = strings.TrimSuffix(sa[i], `"`)
		sa[i] = strings.TrimPrefix(sa[i], `"`)

		sa[i] = strings.TrimSuffix(sa[i], "`")
		sa[i] = strings.TrimPrefix(sa[i], "`")

		sa[i] = strings.TrimSuffix(sa[i], "[]")
		sa[i] = strings.TrimPrefix(sa[i], "]")
	}
	return sa
}

func (d *DB) getTableNameTextOnly(tableName string) string {
	tableName = strings.TrimSpace(tableName)
	tableName = strings.TrimPrefix(tableName, "[")
	tableName = strings.TrimSuffix(tableName, "]")

	tableName = strings.TrimPrefix(tableName, `"`)
	tableName = strings.TrimSuffix(tableName, `"`)

	tableName = strings.TrimPrefix(tableName, "`")
	tableName = strings.TrimSuffix(tableName, "`")

	return tableName
}

// getTableColumns returns an array of Column for a
// table. A Column is a full descropton of a table's field.
func (d *DB) getTableColumns(tableName string) []Column {

	var cols []Column
	var sqlx string
	tblLower := strings.ToLower(tableName)

	if !strings.HasPrefix(tableName, "[") && !strings.HasSuffix(tableName, "]") {
		sqlx = fmt.Sprintf(`PRAGMA main.table_xinfo("%s");`, tblLower)
	} else {
		sqlx = fmt.Sprintf(`PRAGMA main.table_xinfo(%s);`, tblLower)
	}

	tblLower = strings.ToLower(d.getTableNameTextOnly(tableName))

	s, _, _ := d.Prepare(sqlx, []any{})
	for {
		rc := C.sqlite3_step(s.cStmt)
		if rc != SQLITE_ROW {
			break
		}
		var c Column
		colName := d.getStmtColVal(&s, 1).(string)
		if int(d.getStmtColVal(&s, 5).(int64)) == 1 {
			c.IsPrimaryKey = true
		}
		c.Name = colName
		c.Ordinal = int(d.getStmtColVal(&s, 0).(int64))
		c.DataType = d.getStmtColVal(&s, 2).(string)
		if d.getStmtColVal(&s, 3).(int64) == 1 {
			c.NotNULL = true
		}
		c.DefaultValue = d.getStmtColVal(&s, 4)
		if d.getStmtColVal(&s, 5).(int64) == 1 {
			c.IsPrimaryKey = true
		}

		c.IsAutoIncrement = false
		c.IsGeneratedAlways = false
		// See if table; look for the sql satement; also do this here (rather than another fucn) [faster]
		// TODO: better to look in select * from sqlite_sequence, if exists?
		sqly := fmt.Sprintf("select [sql] from sqlite_master where lower([name]) = '%s';", tblLower)
		autoInc, _, _ := d.Prepare(sqly, []any{})
		C.sqlite3_step(autoInc.cStmt)
		v := d.getStmtColVal(&autoInc, 0)

		if v != nil {
			sqlSatmt := strings.ToUpper(v.(string))
			pos := strings.Index(sqlSatmt, " AUTOINCREMENT")
			if pos > -1 {
				left := sqlSatmt[:pos]
				s := fmt.Sprintf(" %s ", strings.ToUpper(tblLower))
				if strings.Contains(left, s) && strings.Contains(left, strings.ToUpper(c.Name)) {
					c.IsAutoIncrement = true
				}
			}

			if !c.IsAutoIncrement {
				v := strings.Split(sqlSatmt, "\n")
				for i := 0; i < len(v); i++ {
					if strings.Contains(v[i], "PRIMARY KEY") && strings.Contains(v[i], "AUTOINCREMENT") {
						s := strings.ReplaceAll(v[i], "PRIMARY KEY", "")
						s = strings.ReplaceAll(s, "AUTOINCREMENT", "")
						s = strings.ReplaceAll(s, "(", "")
						s = strings.ReplaceAll(s, ")", "")
						s = strings.ReplaceAll(s, `"`, "")
						s = strings.ReplaceAll(s, "\t", "")
						s = strings.TrimSpace(s)
						if strings.EqualFold(s, colName) {
							c.IsAutoIncrement = true
						}
						continue
					}
				}
			}

			if !c.IsAutoIncrement {
				sqlSatmt = v.(string)
				sqlSatmt = prunDoubleSpace(sqlSatmt)
				phrase := fmt.Sprintf(`PRIMARY KEY("%s" AUTOINCREMENT)`, colName)
				if strings.Contains(sqlSatmt, phrase) {
					c.IsAutoIncrement = true
				}
			}
			if !c.IsAutoIncrement {

				upperColName := strings.ToUpper(colName)

				v := strings.Split(strings.ReplaceAll(sqlSatmt, "\t", ""), "\n")
				for i := 0; i < len(v); i++ {
					exitFor := false
					v[i] = strings.TrimSpace(v[i])

					v2 := strings.Split(v[i], ",")
					for j := 0; j < len(v2); j++ {
						if strings.Contains(v2[j], upperColName) &&
							strings.Contains(v2[j], "GENERATED ALWAYS AS") {
							c.IsGeneratedAlways = true
							exitFor = true
							break
						}
					}
					if exitFor {
						break
					}
				}

			}
		}
		C.sqlite3_finalize(autoInc.cStmt)

		cols = append(cols, c)
	}
	C.sqlite3_finalize(s.cStmt)

	return cols
}

// getTableNameFromSQLQuery parses the table name from an SQL statement.
func getTableNameFromSQLQuery(sqlQuery string) string {

	sqlQueryLower := strings.TrimSpace(strings.ToLower(sqlQuery))
	sqlQueryLower = strings.ReplaceAll(sqlQueryLower, "(select ", "")
	sqlQueryLower = strings.ReplaceAll(sqlQueryLower, "_rowid_", "")
	sqlQueryLower = prunDoubleSpace(sqlQueryLower)
	sqlQueryLower = strings.TrimSpace(sqlQueryLower)

	for i := 0; i < 100; i++ {
		pos := strings.Index(sqlQueryLower, "from ")
		if pos < 0 {
			pos = strings.Index(sqlQueryLower, "from(")
			if pos < 0 {
				pos = strings.Index(sqlQueryLower, "from (")
			}
		}

		if pos < 0 {
			break
		}

		sqlQueryLower = strings.TrimSpace(sqlQueryLower[pos+1+len("from"):])
		sqlQueryLower = strings.TrimSpace(strings.TrimPrefix(sqlQueryLower, "("))
		sqlQueryLower = strings.TrimSpace(strings.TrimSuffix(sqlQueryLower, ")"))
	}

	x := strings.Split(sqlQueryLower, " ")
	tblName := x[0] // the top element is the table name
	tblName = strings.TrimSpace(strings.TrimPrefix(tblName, "("))
	tblName = strings.TrimSpace(strings.TrimSuffix(tblName, ")"))

	return tblName
}

// waiForQueryResult waits for the exec callback to write the result
// (for one row). It retreives the result-row when available and
// adds it to the global map; it will remove the cached array-element
// once query is done and ready to be returned to the result-set.
func (d *DB) waiForQueryResult(queryID *string, timeoutSec int) QueryResult {

	var q QueryResult

	if timeoutSec == 0 {
		// this is a rough estimation of the below cycle.
		timeoutSec = 1500000 * 60 * 60 * 5 // 5 minutes; UNDONE: make the timeout configurable
	}

	j := 0
	for {
		if j > timeoutSec {
			q.Err = errors.New("query timed out")
			return q
		}
		var qLen int
		// only lock the counter, otherwise
		// the below loop will run slowly.
		mCMutex.Lock()
		qLen = len(mResultQueue)
		mCMutex.Unlock()

		for i := 0; i < qLen; i++ {
			if i >= len(mResultQueue) {
				break
			}
			if i < len(mResultQueue) && len(mResultQueue) > 0 &&
				mResultQueue[i].QueryID == *queryID &&
				(mResultQueue[i].Finished || mResultQueue[i].Err != nil) {
				// save in a variable immediately; o rapid calls, the
				// index goes out-of-bound!
				mq := mResultQueue[i]
				q = QueryResult{
					SeqNo:       mq.SeqNo,
					ResultTable: mq.Result,
					QueryID:     mq.QueryID,
					Err:         mq.Err, /* out-of-bound array index err may heppen if not using the var above */
					TimeStarted: mq.TimeStarted,
					TimeEnded:   mq.TimeEnded}

				// this statement is repeated here for
				// rappid calls
				if i >= len(mResultQueue) {
					break
				}

				if len(q.ResultTable) > 0 {
					colLen := len(q.ResultTable[0])
					q.Columns = make([]Column, colLen)
					p := 0
					for k, v := range q.ResultTable[0] {
						q.Columns[p] = Column{
							Name:     k,
							Ordinal:  p,
							DataType: GetSQLiteDataType(v),
						}
						p++
						if p >= colLen {
							break
						}
					}
				}

				if len(mResultQueue) > 0 && i < len(mResultQueue) {
					mResultQueue[i].Received = true
					d.removeFromResultQueue(*queryID)
				}
				return q
			}
		}
		j++
	}
}

// getStmtColVal gets a single column in an SQL
// statement based on its ordinal position.
func (d *DB) getStmtColVal(s *Stmt, colIndx int) any {
	var v any
	var cVal *C.char
	defer C.free((unsafe.Pointer(cVal)))

	colType := C.sqlite3_column_type(s.cStmt, C.int(colIndx))

	switch colType {
	case SQLITE_NULL:
		v = nil

	case SQLITE_FLOAT:
		v = float64(C.sqlite3_column_double(s.cStmt, C.int(colIndx)))

	case SQLITE_BLOB:
		b := C.sqlite3_column_blob(s.cStmt, C.int(colIndx))
		if b == nil {
			v = []byte{}
		} else {
			n := C.sqlite3_column_bytes(s.cStmt, C.int(colIndx))
			v = C.GoBytes(b, n)
		}

	case SQLITE_TEXT:
		cVal = (*C.char)(unsafe.Pointer(C.sqlite3_column_text(s.cStmt, C.int(colIndx))))
		v = C.GoString(cVal)

	case SQLITE_INTEGER:
		v = int64(C.sqlite3_column_int64(s.cStmt, C.int(colIndx)))
	}

	return v
}

// getSQLiteErr gets the sqilte3 error message based
// on a reutrn code.
func getSQLiteErr(res C.int, dbHwnd *C.sqlite3) error {

	if int(res) == 0 {
		return nil
	} else {
		return errors.New(C.GoString(C.sqlite3_errmsg(dbHwnd)))
	}
}

// processRequestQueue loops thru the global result queue and
// calls execWithResults() on requests that have not yet been
// started.
func (d *DB) processRequestQueue() {
	for i := 0; i < len(mResultQueue); i++ {
		if !mResultQueue[i].Processed && !mResultQueue[i].Received && !mResultQueue[i].Started {
			mResultQueue[i].Started = true
			mResultQueue[i].TimeStarted = time.Now()

			d.execWithResults(mResultQueue[i].QueryID, mResultQueue[i].SQLStmt)
		}
	}
}
