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

	cols := dt.db.GetTableColumns(dt.Name)
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
		query = normalizeSQL(query)
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

			colDesc := d.GetTableColumns(wrk.Name)

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

func (d *DB) PRAGMAList() (*DataTable, error) {
	return d.GetDataTable("PRAGMA pragma_list;")
}

func (d *DB) PRAGMAGetResult(pragmaName string) (*DataTable, error) {
	return d.GetDataTable(fmt.Sprintf("PRAGMA %s", pragmaName))
}

type DataTableOp struct {
	MaxTries      int
	MillSecToWait int

	db *DB
}

// IDataTableOp gets resuls of a DataTable;
// with and without context. Note that it should be
// used internally and without syncing mutex (the
// caller must do that).
type IDataTableOp interface {
	getWithContext(ctx context.Context, query string, placeHolders ...any) (*DataTable, error)
	get(query string, placeHolders ...any) DataTable
}

func (d *DataTableOp) getWithContext(ctx context.Context, query string, placeHolders ...any) (*DataTable, error) {
	var wrkRes DataTable

	c := make(chan DataTable)
	go func(ctx context.Context) {
		select {
		case c <- d.get(query, placeHolders...):
		case <-ctx.Done():
			// should not get here, if func succeeds
			wrkRes.Err = ctx.Err()
			c <- wrkRes
		}
	}(ctx)
	wrkRes = <-c
	close(c)

	wrkRes.TimeEnded = time.Now()

	return &wrkRes, wrkRes.Err
}

func (d *DataTableOp) get(query string, placeHolders ...any) DataTable {
	var wrkRes DataTable

	if d == nil || d.db.Closed {
		wrkRes.Err = errors.New("database is not open")
		return wrkRes
	}

	// query = strings.TrimSpace(query)
	// if len(query) < 7 {
	// 	wrkRes.Err = errors.New("invalid query")
	// 	return wrkRes
	// }

	// PRAGMA is a ligit select statement becuase
	// it returns singlevalue and rows and columns.
	// if !strings.EqualFold(query[:6], "SELECT") && !strings.EqualFold(query[:6], "PRAGMA") {
	// 	wrkRes.Err = errors.New("invalid query")
	// 	return wrkRes
	// }

	if !d.db.StmtReadOnly(query) {
		wrkRes.Err = errors.New("invalid query")
		return wrkRes
	}

	if !d.db.isInMemory {
		_, err := os.Stat(d.db.filePath)
		if os.IsNotExist(err) {
			wrkRes.Err = errors.New("database is not open")
			return wrkRes
		}
	}

	// now  add the global counter
	GetDataTableSeqNo++

	c := make(chan DataTable)
	go func() {
		var wrk DataTable
		query = normalizeSQL(query)
		// var isDBLockedErr bool
		wrk.db = d.db
		wrk.SeqNo = GetDataTableSeqNo
		// tries := 0
		wrk.TimeStarted = time.Now()
		//tryAgain:
		//tries++
		s, pzTail, err := d.db.Prepare(query, placeHolders)
		if err == nil {
			wrk.SQLTail = pzTail
			wrk.Name = getTableNameFromSQLQuery(query)
			colCnt := int(C.sqlite3_column_count(s.cStmt))
			for i := range colCnt {
				// a column name from the query (cStmt) could be
				// an alias, which would not be in the schema.
				colName := C.GoString(C.sqlite3_column_name(s.cStmt, C.int(i)))
				wrk.Columns = append(wrk.Columns, Column{
					Name: colName, Ordinal: i})
			}

			colDesc := d.db.GetTableColumns(wrk.Name)

			// set the columns first; dynamic + IsPrimaryKey
			for i := range wrk.Columns {
				for k := range colDesc {
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
					m[wrk.Columns[i].Name] = d.db.getStmtColVal(&s, i)

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
			// isDBLockedErr = strings.Contains(wrk.Err.Error(), "database is locked")

			// if tries <= d.MaxTries && wrk.Err != nil && isDBLockedErr {
			// 	// Note: this must be a go function and not a C sleep()
			// 	time.Sleep(time.Duration(d.MillSecToWait) * time.Millisecond)
			// 	// goto tryAgain

			// }
		}
		c <- wrk
	}()
	wrkRes = <-c
	close(c)

	wrkRes.TimeEnded = time.Now()

	GetDataTableSeqNo--

	return wrkRes
}

func (d *DB) GetDataTableWithContext(ctx context.Context, query string, placeHolders ...any) (*DataTable, error) {

	mCMutex.Lock()
	defer mCMutex.Unlock()

	var itbl IDataTableOp = &DataTableOp{MaxTries: 3, MillSecToWait: 150, db: d}

	dt, err := itbl.getWithContext(ctx, query, placeHolders...)

	return dt, err
}

// -------------
type callQueItem struct {
	SQLTxt                   string
	Args                     []any
	FuncName                 string
	ResultTable              *DataTable
	ResultExec               Result
	ResultExecLastInsertedID int64
	ResultExecRowsAfffected  int64
	ResultExecScalare        any
	Error                    string
}

type exeQueue struct {
	ExeQueue []execQueItem
}
type execQueItem struct {
	QueueID     string
	SQLTxt      string
	Args        []any
	FuncName    string
	ResultTable *DataTable
	ResultRows  *Rows
	// RSNext                   *gob.Decoder
	ResultNext               bool
	ResultNextData           []byte
	ResultExecLastInsertedID int64
	ResultExecRowsAfffected  int64
	ResultExecScalare        any
	Error                    string

	Done bool
}

func (e *exeQueue) Delete(queueID string) {
	mCMutex.Lock()
	defer mCMutex.Unlock()

	for i := range e.ExeQueue {
		if e.ExeQueue[i].QueueID == queueID {
			e.ExeQueue[len(e.ExeQueue)-1], e.ExeQueue[i] = e.ExeQueue[i], e.ExeQueue[len(e.ExeQueue)-1]
			e.ExeQueue = e.ExeQueue[:len(e.ExeQueue)-1]
			return
		}
	}
}

func (e *exeQueue) Add(eqItm execQueItem) {
	if e == nil {
		e = new(exeQueue)
		e.ExeQueue = make([]execQueItem, 0)
	}
	e.ExeQueue = append(e.ExeQueue, eqItm)
}

func (e *exeQueue) Get(queueID string) execQueItem {
	mCMutex.Lock()
	defer mCMutex.Unlock()
	for i := range e.ExeQueue {
		if e.ExeQueue[i].QueueID == queueID {
			return e.ExeQueue[i]
		}
	}
	return execQueItem{}
}

/*
func (d *DB) processCallQueueQuery() {
	mCMutex.Lock()
	defer mCMutex.Unlock()
	for i := range len(d.eQueueQuery.ExeQueue) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			switch d.eQueueQuery.ExeQueue[i].FuncName {
			case "Query":
				r, err := d.query(d.eQueueQuery.ExeQueue[i].SQLTxt, d.eQueueQuery.ExeQueue[i].Args...)
				if err != nil {
					d.eQueueQuery.ExeQueue[i].Error = err.Error()
				} else {
					d.eQueueQuery.ExeQueue[i].ResultRows = r
				}
			case "Next":
				// d.eQueueQuery.ExeQueue[i].Done = true
				// d.eQueueQuery.ExeQueue[i].ResultRows.next()
				d.eQueueQuery.ExeQueue[i].ResultNext = d.eQueueQuery.ExeQueue[i].ResultRows.next()
				//fmt.Println("xxxxx")
				/ *
					//var rs *Rows
					var buf *bytes.Reader
					buf = bytes.NewReader(d.eQueueQuery.ExeQueue[i].ResultNextData)
					d.eQueueQuery.ExeQueue[i].RSNext = gob.NewDecoder(buf)
					//d.eQueueQuery.ExeQueue[i].RSNext.Decode(rs)
					// d.eQueueQuery.ExeQueue[i].ResultNext = rs.next()
					// qItm.Value.Decode(&ss)
					//next()
				* /
			}
			d.eQueueQuery.ExeQueue[i].Done = true
		}(&wg)
		wg.Wait()
	}
}
*/

/*
func (d *DB) processCallQueueRead() {
	mCMutex.Lock()
	defer mCMutex.Unlock()
	for i := range len(d.eQueueRead.ExeQueue) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			switch d.eQueueRead.ExeQueue[i].FuncName {
			case "GetDataTable":
				var itbl IDataTableOp = &DataTableOp{MaxTries: 3, MillSecToWait: 150, db: d}
				dt := itbl.get(d.eQueueRead.ExeQueue[i].SQLTxt, d.eQueueRead.ExeQueue[i].Args...)
				d.eQueueRead.ExeQueue[i].ResultTable = &dt
				if dt.Err != nil {
					d.eQueueRead.ExeQueue[i].Error = dt.Err.Error()
				}
			}
			d.eQueueRead.ExeQueue[i].Done = true
		}(&wg)
		wg.Wait()
	}
}
*/

/*
func (d *DB) processCallQueueWrite() {
	mCMutex.Lock()
	defer mCMutex.Unlock()
	for i := range len(d.eQueueWrite.ExeQueue) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			switch d.eQueueWrite.ExeQueue[i].FuncName {
			case "Exec":
				res := d.exec(d.eQueueWrite.ExeQueue[i].SQLTxt, d.eQueueWrite.ExeQueue[i].Args...)
				d.eQueueWrite.ExeQueue[i].ResultExecLastInsertedID, _ = res.LastInsertId()
				d.eQueueWrite.ExeQueue[i].ResultExecRowsAfffected, _ = res.RowsAffected()
				if res.Error() != nil {
					d.eQueueWrite.ExeQueue[i].Error = res.Error().Error()
				}
			case "ExecScalare":
				var errx error
				d.eQueueWrite.ExeQueue[i].ResultExecScalare, errx = d.executeScalare(d.eQueueWrite.ExeQueue[i].SQLTxt, d.eQueueWrite.ExeQueue[i].Args...)
				if errx != nil {
					d.eQueueWrite.ExeQueue[i].Error = errx.Error()
				}
			}
			d.eQueueWrite.ExeQueue[i].Done = true
		}(&wg)
		wg.Wait()
	}

	/ *
		//----------------------
		sessionTypeName := "gosqlite.callQueItem"
		cnt := d.callQueue.Count(Write)
		for range cnt {
			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				qItm := d.callQueue.GetItem(Write, nil)
				if qItm.TypeName == "" {
					return
				}
				var ss callQueItem
				json.Unmarshal(qItm.Value, &ss)
				// qItm.Value.Decode(&ss)
				switch qItm.TypeName {
				case sessionTypeName:
					switch ss.FuncName {
					case "Exec":
						res := d.exec(ss.SQLTxt, ss.Args...)
						ss.ResultExec = res
						ss.ResultExecLastInsertedID, _ = res.LastInsertId()
						ss.ResultExecRowsAfffected, _ = res.RowsAffected()
						if ss.ResultExec.Error() != nil {
							ss.Error = ss.ResultExec.Error().Error()
						}
					case "ExecuteScalare":
						var errx error
						ss.ResultExecScalare, errx = d.executeScalare(ss.SQLTxt, ss.Args...)
						if errx != nil {
							ss.Error = errx.Error()
						}
					}
					d.callQueue.UpdateItem(ss, sessionTypeName, qItm.QueueID, Write, nil)
				}

			}(&wg)

			wg.Wait()
		}
	* /
}
*/
/*
func (d *DB) processCallQueueRead() {

	mCMutex.Lock()
	defer mCMutex.Unlock()

	sessionTypeName := "gosqlite.callQueItem"
	cnt := d.callQueue.Count(Read)
	for range cnt {
		// for {
		// 	//var wrk callQueItem
		// 	cnt := d.callQueue.Count(Read)
		// 	if cnt == 0 {
		// 		return
		// 	}

		var wg sync.WaitGroup

		wg.Add(1)

		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			qItm := d.callQueue.GetItem(Read, nil)

			if qItm.TypeName == "" {
				// No items to process, wait just a bit
				// Sleep(50)
				// wg.Done()
				return
			}

			var ss callQueItem
			json.Unmarshal(qItm.Value, &ss)
			// qItm.Value.Decode(&ss)
			switch qItm.TypeName {
			case sessionTypeName:
				switch ss.FuncName {
				case "GetDataTable":
					var itbl IDataTableOp = &DataTableOp{MaxTries: 3, MillSecToWait: 150, db: d}
					dt := itbl.get(ss.SQLTxt, ss.Args...)
					ss.ResultTable = &dt
					if dt.Err != nil {
						ss.Error = dt.Err.Error()
					}
				}
			}

			d.callQueue.UpdateItem(ss, sessionTypeName, qItm.QueueID, Read, nil)

		}(&wg)

		wg.Wait()
	}
}
*/
/*
func (d *DB) processCallQueue() {
	mCMutex.Lock()
	defer mCMutex.Unlock()

	sessionTypeName := "gosqlite.callQueItem"

	for {
		//var wrk callQueItem
		cnt := d.callQueue.Count()
		if cnt == 0 {
			return
		}
		qItm := d.callQueue.GetItem()
		//////////////////
		log.Println("ROWID:", qItm.QueueID)
		//////////////////

		if qItm.TypeName == "" {
			// No items to process, wait just a bit
			Sleep(50)
			continue
		}

		c := make(chan callQueItem)
		go func(qItm QItemReceived) {
			var ss callQueItem
			json.Unmarshal(qItm.Value, &ss)
			switch qItm.TypeName {
			case sessionTypeName:
				// var ss callQueItem
				// qItm.Value.Decode(&ss)
				switch ss.FuncName {
				case "GetDataTable":
					var itbl IDataTableOp = &DataTableOp{MaxTries: 3, MillSecToWait: 150, db: d}
					dt := itbl.get(ss.SQLTxt, ss.Args...)
					ss.ResultTable = &dt
					if dt.Err != nil {
						ss.Error = dt.Err.Error()
					}
				case "Exec":
					res := d.exec(ss.SQLTxt, ss.Args...)
					ss.ResultExec = &res
					err := ss.ResultExec.Error()
					if err != nil {
						ss.Error = err.Error()
					}
				case "ExecuteScalare":
					var errx error
					ss.ResultExecScalare, errx = d.executeScalare(ss.SQLTxt, ss.Args...)
					if errx != nil {
						ss.Error = errx.Error()
					}
				}
				d.callQueue.UpdateItem(ss, sessionTypeName, qItm.QueueID)
			}
			c <- ss
		}(qItm)
		//wrk = <-c
		close(c)
		//d.callQueue.UpdateItem(wrk, sessionTypeName, qItm.QueueID)
	}
}
*/
/*
// processCallQueue processes all websocket requests
// in FIFO order.
func (d *DB) processCallQueue() {
	mCMutex.Lock()
	defer mCMutex.Unlock()

	sessionTypeName := "gosqlite.callQueItem"
	for {
		cnt := d.callQueue.Count()
		if cnt == 0 {
			return
		}
		qItm := d.callQueue.GetItem()
		if qItm.TypeName == "" {
			// No items to process, wait just a bit
			Sleep(50)
			continue
		}

		var ss callQueItem
		json.Unmarshal(qItm.Value, &ss)
		switch qItm.TypeName {
		case sessionTypeName:
			// var ss callQueItem
			// qItm.Value.Decode(&ss)

			switch ss.FuncName {
			case "GetDataTable":
				var itbl IDataTableOp = &DataTableOp{MaxTries: 3, MillSecToWait: 150, db: d}
				dt := itbl.get(ss.SQLTxt, ss.Args...)
				ss.ResultTable = &dt
				if dt.Err != nil {
					ss.Error = dt.Err.Error()
				}

			case "Exec":
				res := d.exec(ss.SQLTxt, ss.Args...)
				ss.ResultExec = &res
				err := ss.ResultExec.Error()
				if err != nil {
					ss.Error = err.Error()
				}

			case "ExecuteScalare":
				var errx error
				ss.ResultExecScalare, errx = d.executeScalare(ss.SQLTxt, ss.Args...)
				if errx != nil {
					ss.Error = errx.Error()
				}
			}

			d.callQueue.UpdateItem(ss, sessionTypeName, qItm.QueueID)
		}
	}
}
*/

/*
func (d *DB) GetDataTable(query string, placeHolders ...any) (*DataTable, error) {
	if d.eQueueRead == nil {
		d.eQueueRead = new(exeQueue)
	}
	var eq execQueItem
	var dt DataTable
	queueID := util.CreateMD5([]byte(fmt.Sprintf("%s%s", time.Now().String(), query)))
	d.eQueueRead.Add(execQueItem{QueueID: queueID, SQLTxt: query, Args: placeHolders, FuncName: "GetDataTable"})
	d.processCallQueueRead()
	eq = d.eQueueRead.Get(queueID)
	if eq.Error != "" {
		dt.Err = errors.New(eq.Error)
	} else {
		dt = *eq.ResultTable
	}
	d.eQueueRead.Delete(queueID)

	return &dt, dt.Err
}
*/

func (d *DB) GetDataTable(query string, placeHolders ...any) (*DataTable, error) {

	var wrkRes DataTable
	var tries int

	// UNDONE:  make these configurable or let the caller
	// set them via optional args?
	maxTries := 3

	// on "database is locked"
	// time to wait before trying again
	millSecToWait := 150 // time.Duration(150 * time.Millisecond)

	// query = strings.TrimSpace(query)
	// if len(query) < 7 {
	// 	wrkRes.Err = errors.New("invalid query")
	// 	return &wrkRes, wrkRes.Err
	// }

	if !d.StmtReadOnly(query) {
		return new(DataTable), errors.New(("invalid query"))
	}

	// PRAGMA is a ligit select statement becuase
	// it returns singlevalue and rows and columns.
	// if !strings.EqualFold(query[:6], "SELECT") &&
	// 	!strings.EqualFold(query[:6], "PRAGMA") {
	// 	wrkRes.Err = errors.New("invalid query")
	// 	return &wrkRes, wrkRes.Err
	// }

	// if d == nil || d.Closed {
	// 	wrkRes.Err = errors.New("database is not open")
	// 	return &wrkRes, wrkRes.Err
	// }

	// if !d.isInMemory {
	// 	_, err := os.Stat(d.filePath)
	// 	if os.IsNotExist(err) {
	// 		wrkRes.Err = errors.New("database is not open")
	// 		return &wrkRes, wrkRes.Err
	// 	}
	// }

	GetDataTableSeqNo++

	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan DataTable)
	go func() {
		var wrk DataTable
		var isDBLockedErr bool
		wrk.db = d
		wrk.SeqNo = GetDataTableSeqNo

		tries = 0
		wrk.TimeStarted = time.Now()
		query = normalizeSQL(query)

	tryAgain:
		tries++
		s, pzTail, err := d.Prepare(query, placeHolders)
		if err == nil {
			wrk.SQLTail = pzTail
			wrk.Name = getTableNameFromSQLQuery(query)
			colCnt := int(C.sqlite3_column_count(s.cStmt))
			for i := range colCnt {
				// a column name from the query (cStmt) could be
				// an alias, which would not be in the schema.
				colName := C.GoString(C.sqlite3_column_name(s.cStmt, C.int(i)))
				wrk.Columns = append(wrk.Columns, Column{
					Name: colName, Ordinal: i})
			}

			colDesc := d.GetTableColumns(wrk.Name)

			// set the columns first; dynamic + IsPrimaryKey
			for i := range wrk.Columns {
				for k := range colDesc {
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
				for i := range wrk.Columns {
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
			isDBLockedErr = strings.Contains(wrk.Err.Error(), "database is locked")

			if tries <= maxTries && wrk.Err != nil && isDBLockedErr {
				C.sqlite3_sleep(C.int(millSecToWait))
				goto tryAgain
			}
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

/*
func (d *DB) GetDataTable_KEEP$(query string, placeHolders ...any) (*DataTable, error) {
	if d.eQueueRead == nil {
		d.eQueueRead = new(exeQueue)
	}
	var eq execQueItem
	var dt DataTable
	queueID := util.CreateMD5([]byte(fmt.Sprintf("%s%s", time.Now().String(), query)))
	d.eQueueRead.Add(execQueItem{QueueID: queueID, SQLTxt: query, Args: placeHolders, FuncName: "GetDataTable"})
	d.processCallQueueRead()
	eq = d.eQueueRead.Get(queueID)
	if eq.Error != "" {
		dt.Err = errors.New(eq.Error)
	} else {
		dt = *eq.ResultTable
	}
	d.eQueueRead.Delete(queueID)
	return &dt, dt.Err

	/ *
		if d.callQueue == nil {
			// no database has been successfully opoen
			return &DataTable{}, errors.New("database not open")
		}

		var rowID int64
		var dt DataTable
		var wg sync.WaitGroup

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			if wg != nil {
				defer wg.Done()
			}

			rowID, _ = d.callQueue.AddItem(callQueItem{
				SQLTxt:   query,
				Args:     placeHolders,
				FuncName: "GetDataTable",
			}, "gosqlite.callQueItem", Read)

			d.processCallQueueRead()

			qItm := d.callQueue.GetItemByID(rowID, Read)

			var ss callQueItem
			// if qItm.Value != nil {
			if len(qItm.Value) > 0 {
				// qItm.Value.Decode(&ss)

				/////////////////////////////////
				// fmt.Println(string(qItm.Value))
				/////////////////////////////////

				json.Unmarshal(qItm.Value, &ss)

				if ss.ResultTable != nil {
					dt = *ss.ResultTable
					if ss.Error != "" {
						dt.Err = errors.New(ss.Error)
					}
				}
			}
			// else {
			// 	log.Println("trying again: ", qItm.QueueID)
			// 	goto lbl
			// }

			// if ss.ResultTable != nil {
			// 	dt = *ss.ResultTable
			// }

			d.callQueue.DeleteItem(rowID, Read)

			//-------------------------

		}(&wg)

		wg.Wait()
		//-------------------------

		return &dt, dt.Err
	* /
}
*/
