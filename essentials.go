// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

package gosqlite

//#include <stdio.h>
//#include <stdlib.h>
//#include "sqlite3.h"
import "C"
import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

func (rs *Rows) DescribeColumns(tableName string) []Column {

	tableName = strings.TrimSpace(tableName)
	if len(tableName) == 0 {
		return []Column{}
	}

	c := rs.db.GetTableColumns("users")

	return c
}

func (rs *Rows) Columns() ([]string, error) {

	if rs == nil || rs.stmt == nil || rs.stmt.cStmt == nil || rs.stmt.released {
		return []string{}, errors.New("row is already closed")
	}

	if len(rs.columns) == 0 {
		for i := range rs.colCount {
			cp := C.sqlite3_column_name(rs.stmt.cStmt, C.int(i))
			rs.columns = append(rs.columns, C.GoString(cp))
		}
	}

	return rs.columns, nil
}

func (rs *Rows) Close() error {

	if rs == nil || rs.stmt.released {
		return nil
	}

	rc := C.sqlite3_finalize(rs.stmt.cStmt)

	if rc != SQLITE_OK {
		return getSQLiteErr(rc, rs.db.DBHwnd)
	}

	rs.stmt.released = true

	return nil
}

func (d *DB) exec(sqlx string, p ...any) (res Result) {
	c := make(chan Result)
	go func() {
		var resw Result
		sqlx = normalizeSQL(sqlx)
		v := strings.Split(sqlx, ";")
		for i := range v {
			query := strings.TrimSpace(v[i])
			if len(query) == 0 {
				continue
			}
			sqlxx := v[i]
			s, _, err := d.Prepare(sqlxx, p)
			if err == nil {
				rc := C.sqlite3_step(s.cStmt)
				if rc != SQLITE_DONE {
					err = getSQLiteErr(rc, d.DBHwnd)
					resw.rowsAffected = -1
					resw.err = err
					resw.intfce = &resw
				} else {
					resw.rowsAffected = int64(C.sqlite3_changes(d.DBHwnd))
					resw.lastInsertId = int64(C.sqlite3_last_insert_rowid(d.DBHwnd))
				}
				C.sqlite3_finalize(s.cStmt)
			} else {
				resw.rowsAffected = -1
				resw.err = err
				resw.intfce = &resw
			}
		}

		c <- resw
	}()
	res = <-c
	close(c)
	return
}

func (d *DB) ExecWithContext(ctx context.Context, query string, placeHolders ...any) Result {
	var wrkRes Result
	c := make(chan Result)
	go func(ctx context.Context) {
		select {
		case c <- d.Exec(query, placeHolders...):
		case <-ctx.Done():
			// should not get here, if func succeeds
			wrkRes.err = ctx.Err()
			c <- wrkRes
		}
	}(ctx)
	wrkRes = <-c
	close(c)

	return wrkRes
}

/*
func (d *DB) Exec(query string, placeHolders ...any) Result {

	var wrk Result

	if d == nil || d.Closed {
		wrk.err = errors.New("database is not open")
		return wrk
	}

	if !d.isInMemory {
		_, err := os.Stat(d.filePath)
		if os.IsNotExist(err) {
			wrk.err = errors.New("database does not exist")
			return wrk
		}
	}

	// if d.callQueue == nil {
	// 	// This is an in-memory database
	// 	res := d.exec(query, placeHolders...)
	// 	return res
	// }

	ExecSeqNo++
	if d.eQueueWrite == nil {
		d.eQueueWrite = new(exeQueue)
	}
	var eq execQueItem
	var res Result
	queueID := util.CreateMD5([]byte(fmt.Sprintf("%s%s", time.Now().String(), query)))
	d.eQueueWrite.Add(execQueItem{QueueID: queueID, SQLTxt: query, Args: placeHolders, FuncName: "Exec"})
	d.processCallQueueWrite()
	eq = d.eQueueWrite.Get(queueID)
	if eq.Error != "" {
		res.err = errors.New(eq.Error)
	}
	res.rowsAffected = eq.ResultExecRowsAfffected
	res.lastInsertId = eq.ResultExecLastInsertedID
	d.eQueueWrite.Delete(queueID)
	ExecSeqNo--
	return res

	/ *
	   //=============================

	   var wg sync.WaitGroup
	   var rowID int64

	   wg.Add(1)

	   	go func(wg *sync.WaitGroup) {
	   		if wg != nil {
	   			defer wg.Done()
	   		}

	   		//--------------------------------
	   		ExecSeqNo++
	   		rowID, _ = d.callQueue.AddItem(callQueItem{
	   			SQLTxt:   query,
	   			Args:     placeHolders,
	   			FuncName: "Exec",
	   		}, "gosqlite.callQueItem", Write)

	   		d.processCallQueueWrite()
	   		qItm := d.callQueue.GetItemByID(rowID, Write)
	   		// if qItm.Value != nil {
	   		if len(qItm.Value) > 0 {
	   			// qItm.Value.Decode(&ss)
	   			json.Unmarshal(qItm.Value, &ss)
	   			//if ss.ResultExec != nil {
	   			r = ss.ResultExec
	   			if ss.Error != "" {
	   				r.err = errors.New(ss.Error)
	   			}
	   			r.lastInsertId = ss.ResultExecLastInsertedID
	   			r.rowsAffected = ss.ResultExecRowsAfffected
	   			///}
	   		}
	   		ExecSeqNo--

	   		d.callQueue.DeleteItem(rowID, Write)
	   		//--------------------------------

	   }(&wg)

	   wg.Wait()

	   return r
	* /
}
*/

func (d *DB) TruncateTable(tblName string) (err error) {
	if !d.TableExists(tblName) {
		err = errors.New("table does not exist")
	} else {
		sqlx := fmt.Sprintf("DELETE FROM %s", tblName)
		res := d.Exec(sqlx)
		if res.Error() != nil {
			err = res.Error()
		} else {
			if d.TableExists("sqlite_sequence") {
				sqlx := "UPDATE sqlite_sequence SET seq = 1 WHERE name = ?"
				res := d.Exec(sqlx, tblName)
				if res.Error() != nil {
					err = res.Error()
				}
			}
			d.Shrink()
		}
	}

	return
}

func (d *DB) doQ() {

	if d.tStmtQBusy {
		return
	}

	d.tStmtQBusy = true
	tryCnt := 0
	timeout := 1000000

	for {
		if len(d.tStmtQ) == 0 {
			break
		}
		item := d.tStmtQ[0]
		res := d.execDo(item.SQLText, item.Args...)
		if res.Error() != nil {
			if res.Error().Error() != "database is locked" || tryCnt > timeout {
				break
			}
			//C.sqlite3_sleep(50)
			tryCnt++
			continue
		}

		if len(d.tStmtQ) > 0 {
			d.tStmtQ = d.tStmtQ[1:]
		}
		//C.sqlite3_sleep(100)
	}

	d.tStmtQBusy = false
}

func (d *DB) doExec(query string, args ...any) Result {
	var res Result

	if d == nil {
		res.err = errors.New("database not initialized")
		return res
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	timeout := 1000000

	for range timeout {
		res = d.execDo(query, args...)
		if res.Error() != nil {
			if res.Error().Error() != "database is locked" {
				break
			}
			continue
		} else {
			break
		}
	}

	return res
}

func (d *DB) ExecNoWait(query string, placeHolders ...any) {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	var tStmt = sqlStmt{
		SQLText: query,
		Args:    placeHolders,
	}

	d.tStmtQ = append(d.tStmtQ, tStmt)
	go d.doQ()
}

func (d *DB) Exec(query string, placeHolders ...any) Result {
	return d.doExec(query, placeHolders...)
}

func (d *DB) execDo(query string, placeHolders ...any) Result {

	var wrk Result
	mCMutex.Lock()
	defer mCMutex.Unlock()

	wrk.rowsAffected = -1

	if d == nil || d.Closed {
		wrk.err = errors.New("database is not open")
		return wrk
	}

	if !d.isInMemory {
		_, err := os.Stat(d.filePath)
		if os.IsNotExist(err) {
			wrk.err = errors.New("database does not exist")
			return wrk
		}
	}

	ExecSeqNo++

	c := make(chan Result)
	go func() {
		var resw Result

		// remove comments here
		query = normalizeSQL(query)

		v := strings.Split(query, ";")
		for i := range v {
			newQuery := strings.TrimSpace(v[i])
			if len(newQuery) == 0 {
				continue
			}
			s, _, err := d.Prepare(newQuery, placeHolders)
			if err == nil && s.cStmt != nil {
				rc := C.sqlite3_step(s.cStmt)
				if rc != SQLITE_DONE {
					err = getSQLiteErr(rc, d.DBHwnd)
					resw.rowsAffected = -1
					resw.err = err
					resw.intfce = &resw
				} else {
					resw.rowsAffected = int64(C.sqlite3_changes(d.DBHwnd))
					resw.lastInsertId = int64(C.sqlite3_last_insert_rowid(d.DBHwnd))
				}

				C.sqlite3_finalize(s.cStmt)

			} else {
				resw.rowsAffected = -1
				resw.err = err
				resw.intfce = &resw
			}
		}

		c <- resw
	}()
	wrk = <-c
	close(c)

	ExecSeqNo--

	return wrk
}

func (r *Result) Error() error {
	return r.err
}

// func (r *Result) PageCount() int64 {
// 	return r.pageCount
// }

func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertId, r.err
}

func (rs *Rows) ScanWithContext(ctx context.Context, args ...any) error {
	var wrkRes error
	c := make(chan error)
	go func(ctx context.Context) {
		select {
		case c <- rs.scanInside(args...):
		case <-ctx.Done():
			// should not get here, if func succeeds
			c <- ctx.Err()
		}
	}(ctx)
	wrkRes = <-c
	close(c)

	return wrkRes
}

func (rs *Rows) scanInside(arg ...any) error {

	if rs == nil || rs.stmt == nil || rs.stmt.released {
		return errors.New("row is already closed")
	}

	mCMutex.Lock()
	defer mCMutex.Unlock()

	argLen := len(arg)
	var cVal *C.char
	defer C.free((unsafe.Pointer(cVal)))

	if argLen > rs.colCount {
		return fmt.Errorf("column/value mismatch: %d columns for %d values", rs.colCount, len(arg))
	}

	for i := range argLen {
		if arg[i] == nil {
			// can't have a nil pointer
			return fmt.Errorf("cannot have a nil pointer (arg %d) for a column value", i)
		}

		if reflect.TypeOf(arg[i]).Kind().String() != "ptr" {
			return fmt.Errorf("arg %d is not a reference to a pointer", i)
		}
	}

	ptrSymbol := '*' // for easier parsing

	for i := range argLen {
		val := rs.db.getStmtColVal(rs.stmt, i)
		if val == nil {
			arg[i] = nil // override any pre-init of the arg

			// must continue or panic
			continue
		}

		t := reflect.TypeOf(val).Kind().String()
		ptrType := reflect.TypeOf(arg[i])

		// let the caller know, in case it's a double+ pointer
		ptrSymbolCount := strings.Count(ptrType.String(), string(ptrSymbol))
		if ptrSymbolCount > 1 {
			ptrClean := strings.ReplaceAll(ptrType.String(), string(ptrSymbol), "")
			return fmt.Errorf("expected *%s not %s in arg %d", ptrClean, ptrType.String(), i)
		}

		// Only TEXT, INTEGER, REAL and BLOB (see: https://www.sqlitetutorial.net/sqlite-data-types).
		// However, time.Time and bool conversions are added for convenience (time/date type is TEXT, and
		// bool is INTEGER in sqlite).

		switch t {
		case "int64":
			// INTEGER
			switch ptrType.String() {
			case "*[]uint8":
				// the column type is BLOB
				// the value has been entered as INTEGER
				arg[i] = val

			case "*int":
				s := new(int)
				s = arg[i].(*int)
				*s = int(val.(int64))
				arg[i] = s

			case "*bool":
				// INTEGER
				s := new(bool)
				s = arg[i].(*bool)
				*s = val.(int64) > 0
				arg[i] = s

			case "*int64":
				s := new(int64)
				s = arg[i].(*int64)
				*s = val.(int64)
				arg[i] = s
			}

		case "float64":
			switch ptrType.String() {
			case "*float64":
				// REAL
				s := new(float64)
				s = arg[i].(*float64)
				*s = val.(float64)
				arg[i] = s

			case "*[]uint8":
				// the column type is BLOB;
				// the value has been entered as REAL
				arg[i] = val
			}

		case "string":
			// TEXT
			switch ptrType.String() {
			case "*string":
				s := new(string)
				s = arg[i].(*string)
				*s = val.(string)
				arg[i] = s

			case "*time.Time":
				s := new(time.Time)
				s = arg[i].(*time.Time)

				// try to conver to time
				*s, _ = ConvertStringToTime(val.(string))
				arg[i] = s

			case "*[]uint8":
				// the column type is BLOB;
				// the value has been entered as TEXT
				arg[i] = val
			}

		case "slice":
			// BLOB
			if ptrType.String() != "*[]uint8" {
				return fmt.Errorf("expected *[]uint8 (*[]byte) not %s in arg %d", ptrType.String(), i)
			}
			s := new([]byte)
			s = arg[i].(*[]byte)
			*s = val.([]byte)
			arg[i] = s

		default:
			return fmt.Errorf("could not recognize pointer of type *%s in arg %d", ptrType.String(), i)
		}
	}

	dirty := false
	for i := range arg {
		if arg[i] != nil {
			dirty = true
			break
		}
	}

	if !dirty {
		return errors.New("scan had no results")
	}

	return nil
}

// Scan scans rows without a context; the calller
// will get a busy or locked error, if the target
// database is being written to.
func (rs *Rows) Scan(arg ...any) error {
	return rs.scanInside(arg...)
}

/*
func (rs *Rows) Scan_COPY(arg ...any) error {
	mCMutex.Lock()
	defer mCMutex.Unlock()

	if rs == nil || rs.stmt == nil || rs.stmt.released {
		return errors.New("row is already closed")
	}

	argLen := len(arg)
	var cVal *C.char
	defer C.free((unsafe.Pointer(cVal)))

	if argLen > rs.colCount {
		return fmt.Errorf("column/value mismatch: %d columns for %d values", rs.colCount, len(arg))
	}

	for i := range argLen {
		if arg[i] == nil {
			// can't have a nil pointer
			return fmt.Errorf("cannot have a nil pointer (arg %d) for a column value", i)
		}

		if reflect.TypeOf(arg[i]).Kind().String() != "ptr" {
			return fmt.Errorf("arg %d is not a reference to a pointer", i)
		}
	}

	ptrSymbol := '*' // for easier parsing

	for i := range argLen {
		val := rs.db.getStmtColVal(rs.stmt, i)
		if val == nil {
			arg[i] = nil // override any pre-init of the arg

			// must continue or panic
			continue
		}

		t := reflect.TypeOf(val).Kind().String()
		ptrType := reflect.TypeOf(arg[i])

		// let the caller know, in case it's a double+ pointer
		ptrSymbolCount := strings.Count(ptrType.String(), string(ptrSymbol))
		if ptrSymbolCount > 1 {
			ptrClean := strings.ReplaceAll(ptrType.String(), string(ptrSymbol), "")
			return fmt.Errorf("expected *%s not %s in arg %d", ptrClean, ptrType.String(), i)
		}

		// Only TEXT, INTEGER, REAL and BLOB (see: https://www.sqlitetutorial.net/sqlite-data-types).
		// However, time.Time and bool conversions are added for convenience (time/date type is TEXT, and
		// bool is INTEGER in sqlite).

		switch t {
		case "int64":
			// INTEGER
			switch ptrType.String() {
			case "*[]uint8":
				// the column type is BLOB
				// the value has been entered as INTEGER
				arg[i] = val

			case "*int":
				s := new(int)
				s = arg[i].(*int)
				*s = int(val.(int64))
				arg[i] = s

			case "*bool":
				// INTEGER
				s := new(bool)
				s = arg[i].(*bool)
				*s = val.(int64) > 0
				arg[i] = s

			case "*int64":
				s := new(int64)
				s = arg[i].(*int64)
				*s = val.(int64)
				arg[i] = s
			}

		case "float64":
			switch ptrType.String() {
			case "*float64":
				// REAL
				s := new(float64)
				s = arg[i].(*float64)
				*s = val.(float64)
				arg[i] = s

			case "*[]uint8":
				// the column type is BLOB;
				// the value has been entered as REAL
				arg[i] = val
			}

		case "string":
			// TEXT
			switch ptrType.String() {
			case "*string":
				s := new(string)
				s = arg[i].(*string)
				*s = val.(string)
				arg[i] = s

			case "*time.Time":
				s := new(time.Time)
				s = arg[i].(*time.Time)

				// try to conver to time
				*s, _ = ConvertStringToTime(val.(string))
				arg[i] = s

			case "*[]uint8":
				// the column type is BLOB;
				// the value has been entered as TEXT
				arg[i] = val
			}

		case "slice":
			// BLOB
			if ptrType.String() != "*[]uint8" {
				return fmt.Errorf("expected *[]uint8 (*[]byte) not %s in arg %d", ptrType.String(), i)
			}
			s := new([]byte)
			s = arg[i].(*[]byte)
			*s = val.([]byte)
			arg[i] = s

		default:
			return fmt.Errorf("could not recognize pointer of type *%s in arg %d", ptrType.String(), i)
		}
	}

	return nil
}
*/

func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}

// prepareFixPlaceholders corrects placeholders.
// It converts elements that arrys into type interface.
// e.g. expected: array of any
// actual: elemnt[0] is an array (of bytes in most cases)
// This could happend when one of the place-holders is
// []byte (i.e. col type is BLOB)
func (d *DB) prepareFixPlaceholders(placeHolders []any) []any {
	var newPL []any
	for i := range placeHolders {
		if placeHolders[i] == nil {
			continue
		}
		if reflect.TypeOf(placeHolders[i]).Kind() == reflect.Slice {
			if reflect.TypeOf(placeHolders[i]).String() == "[]uint8" {
				one := placeHolders[i].([]byte)
				newPL = append(newPL, one)
			} else {
				newPL = append(newPL, placeHolders[i].([]any)...)
			}
		} else {
			newPL = append(newPL, placeHolders[i])
		}
	}

	return newPL
}

// Prepare binds an SQL statement values to its place holders.
// See: https://www.sqlite.org/lang_expr.html#varparam,
// and https://www.sqlite.org/c3ref/bind_blob.html
//
// The return is:
//
//	--a pointer to the prepared statement
//	--unused portion of the sql statement (at the end)
//	--error
func (d *DB) Prepare(
	sqlx string, /* SQL statement, UTF-8 encoded */
	placeHolders []any) (
	Stmt, /* A pointer to the prepared statement */
	string, /* (pzTail) End of parsed string (unused portion of zSql) */
	error) {

	var s Stmt
	s.Query = sqlx
	var ppStmt *C.sqlite3_stmt /* Statement handle */
	var zSql *C.char

	placeHolders = d.prepareFixPlaceholders(placeHolders)

	nByte := len(sqlx)
	zSql = C.CString(sqlx)
	defer C.free(unsafe.Pointer(zSql))

	pzTail := C.CString("")
	defer C.free(unsafe.Pointer(pzTail))

	rc := C.sqlite3_prepare_v2(d.DBHwnd, zSql, C.int(nByte), &ppStmt, &pzTail)
	if rc != SQLITE_OK {
		C.sqlite3_finalize(ppStmt)
		return s, strings.TrimSpace(C.GoString(pzTail)), getSQLiteErr(rc, d.DBHwnd)
	}

	s.cStmt = ppStmt
	var pChr *C.char
	defer C.free(unsafe.Pointer(pChr))

	paramCnt := int(C.sqlite3_bind_parameter_count(ppStmt))

	// prevent array of nil interface to have an effect
	isPlaceHolderEmpty := true
	for i := range placeHolders {
		exitLoop := false
		if placeHolders[i] != nil {
			if reflect.TypeOf(placeHolders[i]).Kind() == reflect.Slice {
				isBlob := reflect.TypeOf(placeHolders[i]).String() == "[]uint8"
				// If isBlob is true, it's a blob;caller has included []bytes in the argument list;
				// so, a nil check is not necessary.
				if !isBlob {
					xa := placeHolders[i].([]any)
					for j := range xa {
						if xa[j] != nil {
							isPlaceHolderEmpty = false
							exitLoop = true
							break
						}
					}
				}
			} else {
				isPlaceHolderEmpty = false
			}
		}
		if exitLoop {
			break
		}
	}

	plcHldLen := len(placeHolders)

	if paramCnt > 0 && !isPlaceHolderEmpty && paramCnt != plcHldLen {
		// this may occur when there is an unexpected char around the:
		// place-holder e.g., '%?%'.
		//
		// Example:
		//   correct:   "... where MyCol LIKE ?","%some string%"
		//   correct:   "... where MyCol LIKE ?1","%some string%"
		//   correct:   "... where MyCol LIKE :AAA","%some string%"
		//   malformed: ".... where MyCol LIKE '%?%'","some string"
		//
		C.sqlite3_finalize(ppStmt)

		return s, strings.TrimSpace(C.GoString(pzTail)),
			errors.New("malformed parameter(s) detected in the sql statement")
	}

	// bind the params
	if !isPlaceHolderEmpty {
		for i := range placeHolders {
			C.sqlite3_reset(ppStmt)

			p := placeHolders[i]

			t := reflect.TypeOf(p)
			if t != nil && t.Kind() == reflect.Slice {
				// ** the args have been passed more than once to get here;
				// ** the target value is inside the array.
				if reflect.TypeOf(p).String() == "[]uint8" {
					vx := p.([]uint8)
					// this a BLOB; convert it to array of bytes
					p = []byte(vx)

				} else {
					vx := p.([]any)
					if len(vx) > 0 {
						p = vx[0]
					}
				}
			}

			switch v := p.(type) {
			case nil:
				// NULL
				rc = C.sqlite3_bind_null(ppStmt, C.int(i+1))

			case uint8:
				// INTEGER
				rc = C.sqlite3_bind_int64(ppStmt, C.int(i+1), C.sqlite3_int64(v))

			case uint:
				// INTEGER
				rc = C.sqlite3_bind_int64(ppStmt, C.int(i+1), C.sqlite3_int64(v))

			case uint32:
				// INTEGER
				rc = C.sqlite3_bind_int64(ppStmt, C.int(i+1), C.sqlite3_int64(v))

			case uint64:
				// INTEGER
				rc = C.sqlite3_bind_int64(ppStmt, C.int(i+1), C.sqlite3_int64(v))

			case int:
				// INTEGER
				rc = C.sqlite3_bind_int64(ppStmt, C.int(i+1), C.sqlite3_int64(v))

			case int32:
				// INTEGER
				rc = C.sqlite3_bind_int64(ppStmt, C.int(i+1), C.sqlite3_int64(v))

			case int64:
				// INTEGER
				rc = C.sqlite3_bind_int64(ppStmt, C.int(i+1), C.sqlite3_int64(v))

			case float32:
				// REAL
				rc = C.sqlite3_bind_double(ppStmt, C.int(i+1), C.double(v))

			case float64:
				// REAL
				rc = C.sqlite3_bind_double(ppStmt, C.int(i+1), C.double(v))

			case bool:
				// 0 OR 1
				// sqlite3 has no bool type; only 0 or 1
				if v {
					rc = C.sqlite3_bind_int(ppStmt, C.int(i+1), 1)

				} else {
					rc = C.sqlite3_bind_int(ppStmt, C.int(i+1), 0)
				}

			case time.Time:
				// TEXT
				// there is no date/time type in sqlite3; only text
				pChr = C.CString(p.(time.Time).String())
				rc = C.sqlite3_bind_text(ppStmt, C.int(i+1), pChr, -1, nil)

			case string:
				// TEXT
				pChr = C.CString(p.(string))
				rc = C.sqlite3_bind_text(ppStmt, C.int(i+1), pChr, -1, nil)

			case []byte:
				// BLOB
				// see if it's empty
				if v == nil {
					C.sqlite3_bind_null(ppStmt, C.int(i+1))

				} else {
					size := len(v)
					rc = C.sqlite3_bind_blob(ppStmt, C.int(i+1), unsafe.Pointer(&v[0]), C.int(size), C.SQLITE_STATIC)
				}

			default:
				return s, C.GoString(pzTail),
					fmt.Errorf("unable to parse place-holder; type %v is not recognized", v)
			}
		}
	}

	if rc != SQLITE_OK {
		C.sqlite3_finalize(ppStmt)
		err := getSQLiteErr(rc, d.DBHwnd)
		if err.Error() == "column index out of range" {
			// more meaning for the caller
			err = errors.New("column does not exist")
		}
		return s, strings.TrimSpace(C.GoString(pzTail)), err
	}

	return s, strings.TrimSpace(C.GoString(pzTail)), nil
}

/*
func (d *DB) query(query string, placeHolders ...any) (*Rows, error) {

	QuerySeqNo++

	// mCMutex.Lock()
	// defer mCMutex.Unlock()

	type result struct {
		rowsPtr Rows
		err     error
	}
	var wrk result
	c := make(chan result)
	go func() {
		var resw result
		s, _, err := d.Prepare(query, placeHolders)
		var rows = Rows{
			stmt:     &s,
			db:       d,
			colCount: int(C.sqlite3_column_count(s.cStmt)),
		}
		rows.intfc = &rows
		resw.rowsPtr = rows
		resw.err = err
		c <- resw
	}()
	wrk = <-c
	close(c)

	QuerySeqNo--

	return &wrk.rowsPtr, wrk.err
}
*/
/*
func (d *DB) Query(query string, placeholders ...any) (*Rows, error) {
	if d.eQueueQuery == nil {
		d.eQueueQuery = new(exeQueue)
	}
	var eq execQueItem
	var r Rows
	var err error
	queueID := util.CreateMD5([]byte(fmt.Sprintf("%s%s", time.Now().String(), query)))
	d.eQueueQuery.Add(execQueItem{QueueID: queueID, SQLTxt: query, Args: placeholders, FuncName: "Query"})
	d.processCallQueueQuery()
	eq = d.eQueueQuery.Get(queueID)
	if eq.Error != "" {
		err = errors.New(eq.Error)
	} else {
		r = *eq.ResultRows
	}
	d.eQueueQuery.Delete(queueID)
	return &r, err
}
*/

func (d *DB) Query(query string, placeHolders ...any) (*Rows, error) {

	QuerySeqNo++

	mCMutex.Lock()
	defer mCMutex.Unlock()

	type result struct {
		rowsPtr Rows
		err     error
	}
	var wrk result
	c := make(chan result)
	go func() {
		query = normalizeSQL(query)
		var resw result
		s, _, err := d.Prepare(query, placeHolders)
		var rows = Rows{
			stmt:     &s,
			db:       d,
			colCount: int(C.sqlite3_column_count(s.cStmt)),
		}
		rows.intfc = &rows
		resw.rowsPtr = rows
		resw.err = err
		c <- resw
	}()
	wrk = <-c
	close(c)

	QuerySeqNo--

	return &wrk.rowsPtr, wrk.err
}

func (rs *Rows) GetInt(colName any) int {
	return int(rs.GetInt64(colName))
}
func (rs *Rows) GetInt64(colName any) int64 {
	var colP any
	var i int

	if colName == nil {
		return 0
	}

	lowerColName := strings.ToLower(colName.(string))

	cols, err := rs.Columns()
	if err != nil {
		return 0
	}

	for i = range cols {
		if strings.ToLower(cols[i]) == lowerColName {
			break
		}
	}

	val := rs.db.getStmtColVal(rs.stmt, i)
	if val == nil {
		return 0
	}

	ptrType := reflect.TypeOf(val)

	// TEXT
	pTpye := ptrType.String()

	// INTEGER
	switch pTpye {

	case "int64":
		colP = val

	case "*[]uint8":
		// the column type is BLOB
		// the value has been entered as INTEGER
		colP = val

	case "*int":
		s := new(int)
		s = colP.(*int)
		*s = int(val.(int64))
		colP = s

	case "*bool":
		// INTEGER
		s := new(bool)
		s = colP.(*bool)
		*s = val.(int64) > 0
		colP = s

	case "*int64":
		s := new(int64)
		s = colP.(*int64)
		*s = val.(int64)
		colP = s
	}

	if colP == nil {
		return 0
	}

	return colP.(int64)
}

func (rs *Rows) GetString(colName any) string {
	var colP any
	var i int

	if colName == nil {
		return ""
	}

	lowerColName := strings.ToLower(colName.(string))

	cols, err := rs.Columns()
	if err != nil {
		return ""
	}

	for i = range cols {
		if strings.ToLower(cols[i]) == lowerColName {
			break
		}
	}

	val := rs.db.getStmtColVal(rs.stmt, i)
	if val == nil {
		return ""
	}

	ptrType := reflect.TypeOf(val)

	// TEXT
	pTpye := ptrType.String()

	switch pTpye {
	case "*string":
		s := new(string)
		s = colP.(*string)
		*s = val.(string)
		colP = s

	case "*time.Time":
		s := new(time.Time)
		s = colP.(*time.Time)

		// try to conver to time
		*s, err = ConvertStringToTime(val.(string))
		if err != nil {
			*s, _ = time.Parse(time.RFC3339, val.(string))
		}
		colP = s

	case "*[]uint8":
		// the column type is BLOB;
		// but the value has been entered as TEXT
		colP = val

	default:
		colP = val
	}

	if colP == nil {
		return ""
	}

	return colP.(string)
}

func (rs *Rows) Next() bool {

	mCMutex.Lock()
	defer mCMutex.Unlock()

	if rs == nil || rs.stmt.cStmt == nil {
		return false
	}

	type result struct {
		stepResult int
	}

	var next result
	c := make(chan result)
	go func() {
		var res result
		rc := C.sqlite3_step(rs.stmt.cStmt)
		res.stepResult = int(rc)
		c <- res
	}()
	next = <-c
	close(c)

	if next.stepResult != SQLITE_ROW {
		C.sqlite3_finalize(rs.stmt.cStmt)
		rs.stmt.released = true
		rs.Close()
		return false
	}

	return true
}

/*
func (rs *Rows) next() bool {
	if rs == nil || rs.stmt.cStmt == nil {
		return false
	}

	type result struct {
		stepResult int
	}

	var next result
	c := make(chan result)
	go func() {
		var res result
		rc := C.sqlite3_step(rs.stmt.cStmt)
		res.stepResult = int(rc)
		c <- res
	}()
	next = <-c
	close(c)

	if next.stepResult != SQLITE_ROW {
		C.sqlite3_finalize(rs.stmt.cStmt)
		rs.stmt.released = true
		rs.Close()
		return false
	}

	return true
}
*/
