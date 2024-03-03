// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

//#include <stdio.h>
//#include <stdlib.h>
//#include "sqlite3.h"
// typedef int (*goFunPtr)();
// char *queueID;
// int set_queue_id(char *qid);
// int set_queue_id(char *qid){
//    queueID = qid;
//    return 0;
//}
// int go_sqlite3_exec_callback(void *NotUsed, int argc, char **argv, char **azColName, char *queryID);
// int getResult(void *NotUsed, int argc, char **argv, char **azColName);
// int getResult(void *NotUsed, int argc, char **argv, char **azColName){
//   go_sqlite3_exec_callback(NotUsed, argc, argv, azColName, queueID);
//   return 0;
// }
import "C"
import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
	"unsafe"
)

// Execute executes an sql statement and returns a single value the error.
// see: https://sqlite.org/cintro.html.
func (d *DB) ExecuteScalare(query string, placeHolders ...any) (any, error) {

	var wrk goChanWork

	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan goChanWork)
	go func() {
		var resw goChanWork
		s, _, err := d.Prepare(query, placeHolders)
		if err == nil {
			colCnt := int(C.sqlite3_column_count(s.cStmt))

			if strings.Contains(query, "_rowid_") && colCnt == 0 {
				colCnt = 1
			}

			if colCnt > 1 {
				resw.returnObj = nil
				resw.err = errors.New("scalare can only return one value")
			} else {
				rc := C.sqlite3_step(s.cStmt)
				if rc != SQLITE_ROW {
					err = getSQLiteErr(rc, d.DBHwnd)
					resw.returnObj = nil
					resw.err = err
				} else {
					resw.err = nil
					resw.returnObj = d.getStmtColVal(&s, 0)
				}
				C.sqlite3_finalize(s.cStmt)
			}
		}
		c <- resw
	}()

	wrk = <-c
	close(c)

	if wrk.err != nil && wrk.err.Error() == "no more rows available" {
		return wrk.returnObj, nil
	}

	return wrk.returnObj, wrk.err
}
func (d *DB) executeNonQueryDo(query string, placeHolders []any) Result {
	var resw Result
	// remove the comments first; as there could be smicolons inside the comment block(s)
	query = strings.ReplaceAll(query, "\n", " ")
	regexptxt2 := `(?)` + regexp.QuoteMeta("/*") + `(.*?)` + regexp.QuoteMeta("*/")
	rx := regexp.MustCompile(regexptxt2)
	m := rx.FindAllString(query, -1)
	for i := 0; i < len(m); i++ {
		query = strings.ReplaceAll(query, m[i], "")
	}
	v := strings.Split(query, ";")
	for i := 0; i < len(v); i++ {
		sqlx := strings.TrimSpace(v[i])
		if len(sqlx) == 0 {
			continue
		}
		s, _, err := d.Prepare(sqlx, placeHolders)
		if err == nil {
			rc := C.sqlite3_step(s.cStmt)
			if rc != SQLITE_DONE {
				err = getSQLiteErr(rc, d.DBHwnd)
				resw.rowsAffected = -1
				resw.err = err
			} else {
				resw.rowsAffected = int64(C.sqlite3_changes(d.DBHwnd))
				resw.lastInsertId = int64(C.sqlite3_last_insert_rowid(d.DBHwnd))
				resw.err = nil
			}
			// release the statement
			C.sqlite3_finalize(s.cStmt)
		} else {
			resw.err = err
		}
	}

	return resw
}
func (d *DB) ExecuteNonQuery(query string, placeHolders ...any) (int64, error) {

	var wrk Result

	ExececuteNonQuerySeqNo++

	mCMutex.Lock()
	defer mCMutex.Unlock()

	if ExececuteNonQuerySeqNo < 2 {
		wrk = d.executeNonQueryDo(query, placeHolders)
		if ExececuteNonQuerySeqNo > 0 {
			ExececuteNonQuerySeqNo--
		}

		return wrk.rowsAffected, wrk.err
	}

	c := make(chan Result)
	go func() {
		var resw Result
		resw = d.executeNonQueryDo(query, placeHolders)

		c <- resw
	}()
	wrk = <-c
	close(c)

	ExececuteNonQuerySeqNo--

	return wrk.rowsAffected, wrk.err
}

// execWithResults sets the queryID and invokes the C.sqlite3_exec().
func (d *DB) execWithResults(queryID string, sqlx string) {
	var errmsg **C.char
	var res C.int
	var err error

	sqlxx := C.CString(sqlx)
	defer C.free(unsafe.Pointer(sqlxx))

	mCMutex.Lock()
	defer mCMutex.Unlock()

	C.set_queue_id(C.CString(queryID))
	res = C.sqlite3_exec(d.DBHwnd, sqlxx, C.goFunPtr(C.getResult), nil, errmsg)
	err = getSQLiteErr(res, d.DBHwnd)

	isDBLocked := false
	if err != nil {
		errTxt := err.Error()
		if errTxt == "database is locked" {
			isDBLocked = true

		} else if errTxt == "out of memory" {
			log.Fatal(errTxt)
		}
	}

	for i := 0; i < len(mResultQueue); i++ {
		if i >= len(mResultQueue) {
			break
		}
		if i < len(mResultQueue) && mResultQueue[i].QueryID == queryID {
			if isDBLocked {
				if i >= len(mResultQueue) {
					break
				}
				// reset
				mResultQueue[i].Started = false
				mResultQueue[i].Finished = false
				mResultQueue[i].ExecResult = -1
				mResultQueue[i].Err = nil
			} else {
				if i >= len(mResultQueue) {
					break
				}
				// if there is an error, there will be more
				// callbacks from the C.sqlite3 for this query.
				mResultQueue[i].Err = err
				mResultQueue[i].ExecResult = int(res)
				mResultQueue[i].TimeEnded = time.Now()
				mResultQueue[i].Processed = true

				// after other processes read the
				// resultset, they many remove the element
				// rapidly from the array; so it may not
				// exist by the time this statement is reached.
				// so, it is important to set the Finshed last.
				mResultQueue[i].Finished = true
				return
			}
			break
		}
	}
}

func (d *DB) Execute(sqlx string) (int64, error) {

	if d.Closed {
		return -1, errors.New("database is not open")
	}

	ExecuteSeqNo++

	var wrk Result

	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan Result)
	go func() {
		var resw Result
		if d.DBHwnd == nil {
			resw.rowsAffected = -1
			resw.err = errors.New("database no longer available")
		} else {
			// execute sql statement(s)
			// remove the comments first; as there could be smicolons inside the comment block(s)
			sqlx = strings.ReplaceAll(sqlx, "\n", " ")
			regexptxt2 := `(?)` + regexp.QuoteMeta("/*") + `(.*?)` + regexp.QuoteMeta("*/")
			rx := regexp.MustCompile(regexptxt2)
			m := rx.FindAllString(sqlx, -1)
			for i := 0; i < len(m); i++ {
				sqlx = strings.ReplaceAll(sqlx, m[i], "")
			}

			v := strings.Split(sqlx, ";")
			for i := 0; i < len(v); i++ {
				query := strings.TrimSpace(v[i])
				if len(query) == 0 {
					continue
				}

				sqlxx := C.CString(query)
				defer C.free(unsafe.Pointer(sqlxx))

				rc := C.sqlite3_exec(d.DBHwnd, sqlxx, C.goFunPtr(C.getResult), nil, nil)
				if rc != SQLITE_OK {
					err := getSQLiteErr(rc, d.DBHwnd)
					resw.rowsAffected = -1
					resw.err = err
				} else {
					resw.rowsAffected = int64(C.sqlite3_changes(d.DBHwnd))
					resw.lastInsertId = int64(C.sqlite3_last_insert_rowid(d.DBHwnd))
					resw.err = nil
				}
			}
		}
		c <- resw
	}()
	wrk = <-c
	close(c)

	ExecuteSeqNo--

	return wrk.rowsAffected, wrk.err
}

func (d *DB) GetResultSet(sqlx string) QueryResult {

	var q QueryResult

	if d.Closed {
		q.Err = errors.New("database is not open")
		return q
	}

	// keep track of connections
	d.getResultSetConnections++
	defer d.decrementActiveConn()

	// global lock for the connSeq increment
	mCMutex.Lock()
	GetResultSetSeqNo++
	queryID := fmt.Sprintf("%d_%s", GetResultSetSeqNo, d.Name)
	mResultQueue = append(mResultQueue, queryResult{
		SeqNo:   GetResultSetSeqNo,
		QueryID: queryID, SQLStmt: sqlx,
		QueryType: DATA_TABLE})
	mCMutex.Unlock()

	if d.getResultSetConnections < 2 {
		d.processRequestQueue()
	}

	return d.waiForQueryResult(&queryID, 0)
}
