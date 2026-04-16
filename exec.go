// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

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
	"strings"
	"time"
	"unsafe"
)

// Execute executes an sql statement and returns a single value the error.
// see: https://sqlite.org/cintro.html.
func (d *DB) ExecuteScalare(query string, placeHolders ...any) (any, error) {

	dt, err := d.GetDataTable(query, placeHolders...)
	if err != nil {
		return nil, err
	}
	if len(dt.Rows) == 0 {
		return nil, nil
	}

	if len(dt.Columns) > 1 {
		return nil, errors.New("scalare can only return one value")
	}

	return dt.Rows[0][dt.Columns[0].Name], nil
}

func (d *DB) executeNonQueryDo(query string, placeHolders []any) Result {
	var resw Result
	var tries int
	query = strings.TrimSpace(query)

	// UNDONE:  make these configurable or let the caller
	// set them via optional args?
	maxTries := 6

	// on "database is locked"
	// time to wait before trying again
	// millSecToWait := time.Duration(150 * time.Millisecond)
	millSecToWait := 150
	sx := strings.ToUpper(removeDoubleSpace(query))
	if d.Closed && !strings.HasPrefix(sx, "PRAGMA") &&
		!strings.Contains(sx, "CREATE TEMP TABLE ") &&
		!strings.Contains(sx, "CREATE TEMPORARY TABLE ") {

		var err error

		d, err = Open(d.filePath, "PRAGMA main.secure_delete = ON")
		if err != nil {
			resw.err = err
			return resw
		}
	}
	query = normalizeSQL(query)
	v := strings.Split(query, ";")
	for i := range len(v) {
		isDBLockedErr := false
		tries = 0
	tryAgain:
		tries++
		// It apears to run faster, if kept in
		// this loop; vs a separate func!
		sqlx := strings.TrimSpace(v[i])
		if len(sqlx) < 3 {
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
			isDBLockedErr = strings.Contains(resw.err.Error(), "database is locked")
			// do not execute the next statement (if any)
			if !isDBLockedErr {
				break
			}
		}
		if tries <= maxTries && resw.err != nil && isDBLockedErr {
			//time.Sleep(millSecToWait * time.Millisecond)
			C.sqlite3_sleep(C.int(millSecToWait))
			goto tryAgain

		} else if resw.err != nil && isDBLockedErr {
			// do not conitue with the next statement (if any)
			break
		}
	}

	return resw
}

func (d *DB) ExecuteNonQuery(query string, placeholders ...any) (int64, error) {
	res := d.Exec(query, placeholders...)
	if res.Error() != nil {
		return -1, res.err
	}
	return res.rowsAffected, nil
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
		if err.Error() == "database is locked" {
			isDBLocked = true

		} else if err.Error() == "out of memory" {
			log.Fatal(err.Error())
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
				// resultset, they may remove the element
				// rapidly from the array; so it may not
				// exist by the time this statement is reached.
				// so, it is important to set the Finshed variable last.
				mResultQueue[i].Finished = true
				return
			}
			break
		}
	}
}

func (d *DB) Execute(sqlx string) (int64, error) {

	var wrk Result

	if d.Closed {
		return -1, errors.New("database is not open")
	}

	ExecuteSeqNo++

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
			sqlx = normalizeSQL(sqlx)
			v := strings.Split(sqlx, ";")
			for i := range v {
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
