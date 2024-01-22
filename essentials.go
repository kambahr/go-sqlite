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
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

func (rs *Rows) Columns() ([]string, error) {
	var c []string

	if rs.stmt.cStmt == nil || rs.stmt.released {
		return c, errors.New("row is already closed")
	}

	colCnt := int(C.sqlite3_column_count(rs.stmt.cStmt))

	for i := 0; i < colCnt; i++ {
		cp := C.sqlite3_column_name(rs.stmt.cStmt, C.int(i))
		c = append(c, C.GoString(cp))
	}

	return c, nil
}

func (rs *Rows) Close() error {

	if rs.stmt.released {
		return nil
	}

	rc := C.sqlite3_finalize(rs.stmt.cStmt)

	if rc != SQLITE_OK {
		return getSQLiteErr(rc, rs.db.DBHwnd)
	}

	rs.stmt.released = true

	return nil
}

func (d *DB) Exec(query string, placeHolders ...any) Result {

	var wrk Result
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

	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan Result)
	go func() {
		var resw Result
		s, _, err := d.Prepare(query, placeHolders)
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

func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertId, r.err
}

func (rs *Rows) Next() bool {

	rc := C.sqlite3_step(rs.stmt.cStmt)
	if rc != SQLITE_ROW {
		rs.stmt.released = true
		rs.Close()
		return false
	}

	return true
}

func (rs *Rows) Scan(dest ...any) error {

	if rs.stmt.released {
		return errors.New("row is already closed")
	}

	var cVal *C.char
	defer C.free((unsafe.Pointer(cVal)))

	for i := 0; i < rs.colCount; i++ {

		dest[i] = rs.db.getStmtColVal(rs.stmt, i)
	}

	return nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}

// Prepare binds sql statement values to its place holders.
// See: https://www.sqlite.org/lang_expr.html#varparam,
//
//	and https://www.sqlite.org/c3ref/bind_blob.html
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

	if paramCnt > 0 && paramCnt != len(placeHolders) {
		// this may occur when there is an expected char around the:
		// place-holder e.g., '%?%'.
		//
		// Example:
		//   correct:       "... where MyCol LIKE ?","%some string%"
		//   also correct:  "... where MyCol LIKE ?1","%some string%"
		//   alco correct:  "... where MyCol LIKE :AAA","%some string%"
		//
		//   malformed:     "... where MyCol LIKE '%?%'","some string"
		//
		C.sqlite3_finalize(ppStmt)

		return s, strings.TrimSpace(C.GoString(pzTail)),
			errors.New("malformed parameter(s) detected in the sql statement")
	}

	// bind the params
	for i := 0; i < len(placeHolders); i++ {
		C.sqlite3_reset(ppStmt)

		p := placeHolders[i]

		t := reflect.TypeOf(p)
		if t.Kind() == reflect.Slice {
			// ** the args have been passed more than once to get here;
			// ** the target value is inside the array.
			if reflect.TypeOf(p).String() == "[]uint8" {
				vx := p.([]uint8)
				// this a BLOB; convert it to array of bytes
				p = []byte(vx)

			} else {
				vx := p.([]any)
				p = vx[0]
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
			return s, C.GoString(pzTail), errors.New("unable to parse place-holder")
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
		C.sqlite3_finalize(s.cStmt)
		c <- resw
	}()
	wrk = <-c
	close(c)

	QuerySeqNo--

	return &wrk.rowsPtr, wrk.err
}
