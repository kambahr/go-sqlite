// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

//#include <stdio.h>
//#include <stdlib.h>
//#include "sqlite3.h"
import "C"
import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

func (d *DB) Base() *DB {
	return d
}

func (d *DB) FilePath() string {
	return d.filePath
}

func (d *DB) GetAttachedDatabases() []AttachedDB {
	var a []AttachedDB

	q := d.GetResultSet("PRAGMA database_list")

	for i := 0; i < len(q.ResultTable); i++ {
		fn := q.ResultTable[i]["name"].(string)
		fp := q.ResultTable[i]["file"].(string)

		if fn != "main" {
			var aa = AttachedDB{
				Name:       fn,
				DBFilePath: fp,
			}
			a = append(a, aa)
		}
	}

	return a
}

func (d *DB) RemoveAllAttachedDBs() error {

	attch := d.GetAttachedDatabases()

	for i := 0; i < len(attch); i++ {
		sqlx := fmt.Sprintf("detach '%s';", attch[i].Name)
		_, err := d.Execute(sqlx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) RemoveAttachedDB(attachName string) error {

	sqlx := fmt.Sprintf("detach '%s';", attachName)
	_, err := d.Execute(sqlx)

	return err
}

func (d *DB) AttachDB(dbFilePathToAttach string, attchName string) (bool, error) {

	sqlx := fmt.Sprintf("attach database '%s' as %s;", dbFilePathToAttach, attchName)
	_, err := d.Execute(sqlx)

	return true, err
}

func (d *DB) CopyDatabase(dbFileToCopyTo string) error {

	if d.Closed {
		return errors.New("database is not open")
	}

	sqlx := fmt.Sprintf(`VACUUM INTO "%s"`, dbFileToCopyTo)

	// execute sql statement
	sqlxx := C.CString(sqlx)
	defer C.free(unsafe.Pointer(sqlxx))

	res := C.sqlite3_exec(d.DBHwnd, sqlxx, nil, nil, nil)
	return getSQLiteErr(res, d.DBHwnd)
}

func (d *DB) Close() error {

	if d == nil || d.Closed {
		return errors.New("database is not open")
	}

	res := C.sqlite3_close(d.DBHwnd)
	err := getSQLiteErr(res, d.DBHwnd)

	if err != nil {
		if !strings.Contains(err.Error(), "bad parameter or other API misuse") {
			return err
		}
	}

	return nil
}

// CloneDB creates a copy of the database using
// the backup utility.
func (d *DB) CloneDB(destPath string) error {

	var err error

	if fileOrDirExists(destPath) {
		return errors.New("destination database file already exists")
	}

	CreateDatabase(destPath)

	err = BackupOnlineDB(d, destPath, 0, /* no wait-time between copying pages*/
		nil, "",
		/*
		   these are optional params to make sure the backup
		   does not conitnue if the database is busy or locked
		*/
		backup_raise_err_on_busy, backup_raise_err_on_dblocked)
	if err != nil {
		return err
	}

	// This (open/close of db) remove the journal files gracefully.
	dx, err := DBGrp.Get(destPath)
	if err != nil {
		return err
	}
	DBGrp.Remove(dx[0])

	return nil
}

func (d *DB) Busy() bool {
	if d == nil || d.DBHwnd == nil {
		return false
	}
	if !d.IsIdle() {
		return true
	}

	return false
}

func (d *DB) DeleteJournalfiles(dbFilePath string) {
	fileName := filepath.Base(dbFilePath)
	fileNameNoExt := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	dir := filepath.Dir(dbFilePath)

	shm := fmt.Sprintf("%s/.%s-sqlite-shm", dir, fileNameNoExt)
	os.Remove(shm)

	wal := fmt.Sprintf("%s/.%s-sqlite-wal", dir, fileNameNoExt)
	os.Remove(wal)

	j := fmt.Sprintf("%s//%s-journal", dir, fileNameNoExt)
	os.Remove(j)
}

func (d *DB) TableExists(tableName string) bool {

	sqlx := fmt.Sprintf(`select COUNT(*) from %s LIMIT 1 OFFSET 0`, tableName)
	_, err := d.ExecuteScalare(sqlx)
	if err != nil {
		return false
	}

	return true
}

func (d *DB) ReOpen() {
	fp := d.FilePath()
	DBGrp.Remove(d)
	d, _ = OpenV2(fp,
		"PRAGMA main.journal_mode = DELETE",
		"PRAGMA main.secure_delete = ON;")

	DBGrp.Add(d)
}

func (d *DB) TurnOffAutoIncrement(tableName string) error {

	if tableName == "" {
		return errors.New("invalid table name")
	}

	if !d.TableExists(tableName) {
		return errors.New("table does not exist")
	}

	// Create a new temp-table with AutoIncrement
	var tblSQL string
	var tblSQLArr []string
	cols := d.getTableColumns(tableName)
	for i := 0; i < len(cols); i++ {
		s := ""
		if cols[i].IsAutoIncrement {
			s = fmt.Sprintf(`"%s" INTEGER`, cols[i].Name)
		} else {
			isNULL := ""
			if cols[i].NotNULL {
				isNULL = "NOT NULL"
			}
			s = fmt.Sprintf(`"%s" %s %s`, cols[i].Name, cols[i].DataType, isNULL)
		}
		tblSQLArr = append(tblSQLArr, s)
	}
	tblSQLTmp := strings.Join(tblSQLArr, ",")
	tblSQLTmp = strings.TrimSuffix(tblSQLTmp, ",")

	tmpTblName := fmt.Sprintf("%s_%s", strings.ReplaceAll(tableName, " ", ""), getRandString())
	tblSQL = fmt.Sprintf(`CREATE TABLE "%s" (%s)`, tmpTblName, tblSQLTmp)

	// ** create the table on the target db
	_, err := d.Execute(tblSQL)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// select the non virtual columns from the original table for the insert statement
	var col []string
	allCols := d.getTableColumns(tableName)
	for i := 0; i < len(allCols); i++ {
		if allCols[i].IsGeneratedAlways {
			continue
		}
		col = append(col, allCols[i].Name)
	}

	insPart := strings.Join(col, `","`)
	insPart = fmt.Sprintf(`("%s")`, insPart)

	insPartSelect := strings.TrimPrefix(insPart, "(")
	insPartSelect = strings.TrimSuffix(insPartSelect, ")")

	insSQL := fmt.Sprintf(`INSERT INTO main."%s" %s SELECT %s FROM main."%s"`, tmpTblName, insPart, insPartSelect, tableName)
	_, err = d.Execute(insSQL)
	if err != nil {
		return err
	}

	err = d.DropTable(tableName)
	if err != nil {
		return err
	}

	err = d.RenameTable(tmpTblName, tableName)

	// re-open the database
	reopenDB(d)

	return err
}

func (d *DB) TurnOnAutoIncrement(tableName string, colName string, reoderAutoInrecValues ...bool) error {

	var err error

	if tableName == "" {
		return errors.New("invalid table name")
	}

	if !d.TableExists(tableName) {
		return errors.New("table does not exist")
	}

	svpntA := getRandString()
	svpntB := getRandString()

	d.Execute("PRAGMA foreign_keys;")
	d.Execute("PRAGMA foreign_keys = '0';")
	d.Execute("PRAGMA collation_list;")
	d.Execute(fmt.Sprintf(`SAVEPOINT "%s";`, svpntA))
	d.Execute(fmt.Sprintf(`SELECT COUNT(*) FROM "main"."%s" WHERE "%s" <> CAST("%s" AS INTEGER) LIMIT 49999 OFFSET 0;`, tableName, colName, colName))
	d.Execute(fmt.Sprintf(`SAVEPOINT "%s";`, svpntB))
	d.Execute("PRAGMA database_list;")
	d.Execute(`SELECT type,name,sql,tbl_name FROM "main".sqlite_master;`)
	d.Execute("SELECT type,name,sql,tbl_name FROM sqlite_temp_master;")
	d.Execute(`SAVEPOINT "RESTOREPOINT";`)

	// Create a new temp-table with AutoIncrement
	var tblSQL string
	var tblSQLArr []string
	cols := d.getTableColumns(tableName)
	for i := 0; i < len(cols); i++ {
		if i == 0 {
			// add the auto-inc column to the top
			tblSQLArr = append(tblSQLArr, fmt.Sprintf(`"%s" INTEGER NOT NULL`, colName))
		}
		if strings.EqualFold(cols[i].Name, colName) {
			continue
		}
		isNULL := ""
		if cols[i].NotNULL {
			isNULL = "NOT NULL"
		}
		s := fmt.Sprintf(`"%s" %s %s`, cols[i].Name, cols[i].DataType, isNULL)
		tblSQLArr = append(tblSQLArr, s)
	}
	// add the primaryKey autoInce last:
	tblSQLArr = append(tblSQLArr, fmt.Sprintf(`PRIMARY KEY("%s" AUTOINCREMENT)`, colName))

	tblSQLTmp := strings.Join(tblSQLArr, ",")
	tblSQLTmp = strings.TrimSuffix(tblSQLTmp, ",")
	tmpTblName := fmt.Sprintf("%s_%s", strings.ReplaceAll(tableName, " ", ""), getRandString())
	tblSQL = fmt.Sprintf(`CREATE TABLE "%s" (%s)`, tmpTblName, tblSQLTmp)

	// ** create the temp table
	_, err = d.Execute(tblSQL)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	// select the non virtual columns from the original table for the insert statement
	var col []string
	allCols := d.getTableColumns(tableName)
	for i := 0; i < len(allCols); i++ {
		if allCols[i].IsGeneratedAlways {
			continue
		}
		if len(reoderAutoInrecValues) > 0 && reoderAutoInrecValues[0] {
			if strings.EqualFold(colName, allCols[i].Name) || allCols[i].IsAutoIncrement {
				continue
			}
		}

		col = append(col, allCols[i].Name)
	}
	insPart := strings.Join(col, `","`)
	insPart = fmt.Sprintf(`("%s")`, insPart)
	insPartSelect := strings.TrimPrefix(insPart, "(")
	insPartSelect = strings.TrimSuffix(insPartSelect, ")")
	insSQL := fmt.Sprintf(`INSERT INTO main."%s" %s SELECT %s FROM main."%s"`, tmpTblName, insPart, insPartSelect, tableName)
	_, err = d.Execute(insSQL)
	if err != nil {
		return err
	}
	d.Execute(`PRAGMA defer_foreign_keys;`)
	d.Execute(`PRAGMA defer_foreign_keys = '1';`)
	d.Execute(fmt.Sprintf(`DROP TABLE "main"."%s"`, tableName))
	d.Execute(fmt.Sprintf(`ALTER TABLE "main"."%s" RENAME TO "%s"`, tmpTblName, tableName))

	d.Execute(`PRAGMA defer_foreign_keys = '0';`)
	d.Execute(fmt.Sprintf(`RELEASE "%s";`, svpntB))
	d.Execute(`PRAGMA database_list;`)
	d.Execute(`SELECT type,name,sql,tbl_name FROM "main".sqlite_master;`)
	d.Execute(`SELECT type,name,sql,tbl_name FROM sqlite_temp_master;`)
	d.Execute(`PRAGMA "main".foreign_key_check`)
	d.Execute(fmt.Sprintf(`RELEASE "%s";`, svpntA))
	d.Execute(`PRAGMA foreign_keys = '1';`)

	DBGrp.Remove(d)

	return err
}

// CopyTableToDatabase copies a table from the current database to another.
// If append is set to true, all rows will be appended to the target table,
// otherwise a new table is created; if the table exists it will be re-name
// (as <table name>_n). If recreateAutoIncrementCol is set to true the
// auto-increment column will be re-create so that the integer numbers are
// reset form 1 to n.
func (d *DB) CopyTableToDatabase(tbleNameToCopy string, targetDBFilePath string,
	dropIfExists bool, append bool, recreateAutoIncrementCol bool) (string, error) {

	var err error
	var dt *DataTable

	// ** get/open the targetDBFilePath
	dbTrgt, err := DBGrp.Get(targetDBFilePath)
	if err != nil {
		return "", err
	}

	newTableName := tbleNameToCopy

	if append {
		// insurance
		dropIfExists = false
	}

	// get a new table name
	if !dropIfExists && !append {
		for i := 1; i < 5000; i++ {
			sqlx := fmt.Sprintf("select name from sqlite_master where type = 'table' and lower(name) = '%s'",
				strings.ToLower(newTableName))

			dt, err := dbTrgt[0].GetDataTable(sqlx)
			if err != nil {
				return "", err
			}
			if len(dt.Rows) < 1 {
				// table does not exists in the target db; it's fine
				break
			}

			newTableName = fmt.Sprintf("%s_%d", tbleNameToCopy, i)
		}
	}

	if dropIfExists {
		// on the target database
		sqlx := fmt.Sprintf("drop table [%s]", tbleNameToCopy)
		dbTrgt[0].Execute(sqlx)
	}

	// ** get the table create sql from this database
	sqlx := fmt.Sprintf("select name,sql from sqlite_master where type = 'table' and lower(name) = '%s'",
		strings.ToLower(tbleNameToCopy))
	dt, err = d.GetDataTable(sqlx)
	if err != nil {
		return "", err
	}
	if len(dt.Rows) < 1 {
		// the source table must exist
		return "", errors.New("table not found")
	}
	tableNameAsIs := dt.Rows[0]["name"].(string)
	tableCreateSQL := dt.Rows[0]["sql"].(string)

	// ** replace that table name with the new name
	v := strings.Split(tableCreateSQL, "(")
	left := v[0]
	left = strings.ReplaceAll(left, tableNameAsIs, newTableName)
	tableCreateSQL = fmt.Sprintf("%s (%s", left, v[1])

	// ** create the table on the target db
	_, err = dbTrgt[0].Execute(tableCreateSQL)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return "", err
	}

	// ** get the auto-increment column, if any
	autoIncmntCol := ""
	if append && recreateAutoIncrementCol {
		cols := dbTrgt[0].getTableColumns(tbleNameToCopy)
		for i := 0; i < len(cols); i++ {
			if cols[i].IsAutoIncrement {
				autoIncmntCol = cols[i].Name
				break
			}
		}

		err = dbTrgt[0].TurnOffAutoIncrement(tbleNameToCopy)
		if err != nil {
			return "", err
		}
	}

	// ** attach the target table to this database
	attachName := "z" + getRandString()
	_, err = d.AttachDB(dbTrgt[0].FilePath(), attachName)
	if err != nil {
		return "", err
	}

	// ** copy the table to target database
	// reopenDB(dbTrgt[0])
	err = d.copyTableToDatabaseBulkInsert(newTableName, tbleNameToCopy, attachName)

	if append && autoIncmntCol != "" && recreateAutoIncrementCol {

		dbTrgt, err = DBGrp.Get(targetDBFilePath)
		if err != nil {
			return "", err
		}
		err = dbTrgt[0].TurnOnAutoIncrement(tbleNameToCopy, autoIncmntCol, true)
		if err != nil {
			return "", err
		}
	}

	// re-open the database
	//reopenDB(dbTrgt[0])
	DBGrp.Remove(dbTrgt[0])

	return newTableName, err
}
func reopenDB(d *DB) error {
	fp := d.FilePath()
	DBGrp.Remove(d)
	dbx, err := DBGrp.Get(fp)
	if err != nil {
		return err
	}
	d = dbx[0]
	return nil
}

func (d *DB) copyTableToDatabaseBulkInsert(newTableName string, tbleNameToCopyFrom string, attachName string) error {

	// select the non virtual columns from the original table for the insert statement
	var col []string
	allCols := d.getTableColumns(tbleNameToCopyFrom)
	for i := 0; i < len(allCols); i++ {
		if allCols[i].IsGeneratedAlways || allCols[i].IsAutoIncrement {
			continue
		}
		col = append(col, allCols[i].Name)
	}

	insPart := strings.Join(col, `","`)
	insPart = fmt.Sprintf(`("%s")`, insPart)

	insPartSelect := strings.TrimPrefix(insPart, "(")
	insPartSelect = strings.TrimSuffix(insPartSelect, ")")

	sqlx := fmt.Sprintf("INSERT INTO %s.`%s`(%s) SELECT %s FROM main.`%s`;",
		attachName, newTableName, insPartSelect, insPartSelect, tbleNameToCopyFrom)
	_, err := d.Execute(sqlx)
	errDetach := d.DetachDB(attachName)
	if err != nil {
		return err
	}

	if err == nil {
		err = errDetach
	}

	return err
}

func (d *DB) DetachDB(attachName string) error {

	sqlx := fmt.Sprintf("DETACH '%s'", attachName)
	_, err := d.Execute(sqlx)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) IsColumnAutoIncrement(colName string, tableName string) (bool, error) {

	sqlx := fmt.Sprintf("select sql from sqlite_master where type = 'table' and lower(name) = '%s'",
		strings.ToLower(tableName))

	dt, err := d.GetDataTable(sqlx)
	if err != nil {
		return false, err
	}
	if len(dt.Rows) == 0 {
		return false, errors.New("unable to query table")
	}
	// exmaple:  SomeID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
	tblCreateSQL := strings.ToUpper(dt.Rows[0]["sql"].(string))
	for i := 0; i < 6; i++ {
		tblCreateSQL = strings.ReplaceAll(tblCreateSQL, "  ", " ")
	}
	trgtStmt := fmt.Sprintf("%s INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT", strings.ToUpper(colName))

	if strings.Contains(tblCreateSQL, trgtStmt) {
		return true, nil
	}

	return false, err
}
func (d *DB) GetInsertSQLFromDataTableRow(dtRow map[string]interface{}, tableName string) (string, error) {

	var cols []string
	for key, _ := range dtRow {
		if key == "_rowid_" {
			continue
		}
		isAutoIncrement, _ := d.IsColumnAutoIncrement(key, tableName)
		if isAutoIncrement {
			continue
		}
		cols = append(cols, key)
	}

	var inserts []string
	var values []string

	// A row must have atleat one none-null value.
	atleastOneNoneNULL := false
	rowColCount := len(cols)

	for i := 0; i < len(cols); i++ {

		// get one value
		if i >= rowColCount {
			break
		}

		iValue := dtRow[cols[i]]

		// column name for insert
		ins := fmt.Sprintf("[%s]", cols[i])

		inserts = append(inserts, ins)
		var p string

		iValue, isValStr := d.isString(iValue)

		if isValStr {
			if iValue == nil {
				p = "NULL"
			} else {
				strVal := fmt.Sprintf("%v", iValue)
				atleastOneNoneNULL = true
				strVal = strings.ReplaceAll(strVal, "<nil>", "")
				p = fmt.Sprintf("'%s'", strings.ReplaceAll(strVal, "'", "''"))
			}
		} else {
			if iValue == nil {
				p = "NULL"
			} else {
				atleastOneNoneNULL = true
				p = fmt.Sprintf("%v", iValue)
			}
		}
		values = append(values, p)
	}
	if len(inserts) == 0 || !atleastOneNoneNULL {
		// Nothing was found to insert
		return "", errors.New("no content to insert")
	}
	sqlx := fmt.Sprintf("insert into %s (%s) values(%s)", tableName, strings.Join(inserts, ","), strings.Join(values, ","))

	return sqlx, nil
}
func (d *DB) isString(value interface{}) (interface{}, bool) {

	strVal := fmt.Sprintf("%v", value)

	if _, err := strconv.ParseFloat(strVal, 64); err == nil {
		return value, false
	}

	if _, err := strconv.Atoi(strVal); err == nil {
		return value, false
	}

	// Date
	if (strings.Contains(strVal, "/") || strings.Contains(strVal, "-")) &&
		strings.Contains(strVal, ":") && strings.Contains(strVal, " ") {
		_, err := ConvertStringToTime(strVal)
		if err == nil {
			// the type is date; in sql statement still needs a single quote
			return strVal, true
		}
	}

	if strings.ToLower(strVal) == "false" {
		return 0, false
	}
	if strings.ToLower(strVal) == "true" {
		return 1, false
	}

	return value, true
}

func (d *DB) EncryptTable(tName string) error {

	/*
	** t1 assum the table is with rowid?
	** t2 create a shadow table
	** any row added to t1 is encrypted
	** for each column a hash of the [unencrypted]
	**
	 */

	return errors.New("NOT YET IMPLEMENTED")
}
func (d *DB) RenameTable(tableName string, newtableName string) error {

	// open the db exclusively
	dbExclusive, err := OpenV2Exclusive(d.FilePath())
	if err != nil {
		return err
	}

	sqlx := fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`", tableName, newtableName)
	_, err = dbExclusive.Execute(sqlx)
	if err != nil {
		return err
	}
	// close the excluse connection
	defer dbExclusive.Close()

	// re-open the database
	reopenDB(d)

	return err
}
func (d *DB) DropView(viewName string) error {

	// get the db file to re-open it
	dbFilePath := d.filePath

	// open the db exclusively
	dbExclusive, err := OpenV2Exclusive(dbFilePath)
	if err != nil {
		return err
	}
	sqlx := fmt.Sprintf(`drop view [%s]`, viewName)
	_, err = dbExclusive.Execute(sqlx)
	if err != nil {
		return err
	}
	// close the excluse connection
	dbExclusive.Close()

	return nil
}
func (d *DB) Vacuum(dbVacuumInto ...string) error {

	if len(dbVacuumInto) > 0 {
		_, err := d.Execute(fmt.Sprintf(`VACUUM "main" into "%s"`, dbVacuumInto[0]))
		if err != nil {
			return err
		}

		return nil
	}

	_, err := d.Execute(`VACUUM "main"`)
	if err != nil {
		return err
	}
	_, err = d.Execute(`VACUUM "temp"`)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) Shrink() error {

	return d.Vacuum()
}

func (d *DB) DropTable(tableName string, vaccumAfter ...bool) error {

	// get the db file to re-open it
	dbFilePath := d.filePath

	// open the db exclusively
	dbExclusive, err := OpenV2Exclusive(dbFilePath)
	if err != nil {
		return err
	}

	sqlx := fmt.Sprintf(`drop table [%s]`, tableName)
	_, err = dbExclusive.Execute(sqlx)
	if err != nil {
		return err
	}
	dbExclusive.Close()

	if len(vaccumAfter) > 0 && vaccumAfter[0] {
		d.Vacuum()
	}

	return nil
}

func (d *DB) Describe() DBStat {

	var dx DBStat
	stats, err := os.Stat(d.filePath)
	if err == nil {
		dx.FilePath = d.filePath
		dx.Name = d.Name
		// Get the db size
		st := ""
		var xf float64
		var s float64 = float64(stats.Size())
		var u float64 = float64(1024)

		if s < (1000 * 1000) {
			xf = s / u
			xf = roundNumber(xf, 2)
			st = fmt.Sprintf("%v KB", xf)

		} else if s >= 1000 && s < (1000*1000*1000) {
			xf = s / u / u
			xf = roundNumber(xf, 2)
			st = fmt.Sprintf("%v MB", xf)

		} else {
			xf = s / u / u / u
			xf = roundNumber(xf, 2)
			st = fmt.Sprintf("%v GB", xf)
		}
		dx.LastModified = stats.ModTime()
		dx.Size = st

		// Get the objects
		sqlx := "select * from sqlite_master order by [type] desc;"
		rs := d.GetResultSet(sqlx)
		m := rs.ResultTable
		if err != nil {
			return DBStat{}
		} else if m != nil && len(m) > 0 {

			// get tables
			var tbls []Table
			var indxes []Index
			var views []View
			var trgs []Trigger
			for j := 0; j < len(m); j++ {
				oName := m[j]["name"].(string)
				oSql := m[j]["sql"].(string)
				rp, _ := m[j]["rootpage"].(int64)
				if m[j]["type"].(string) == "table" {
					tbls = append(tbls, Table{
						Name:      oName,
						Columns:   d.getTableColumns(oName),
						RootPage:  uint(rp),
						CreateSQL: oSql,
					})

				} else if m[j]["type"].(string) == "view" {
					views = append(views, View{
						Name:      oName,
						CreateSQL: oSql,
						RootPage:  uint(rp),
					})

				} else if m[j]["type"].(string) == "trigger" {
					trgs = append(trgs, Trigger{
						Name:      oName,
						CreateSQL: oSql,
						RootPage:  uint(rp),
					})

				} else if m[j]["type"].(string) == "index" {
					indxes = append(indxes, Index{
						Name:      oName,
						Columns:   d.getIndexColumns(oSql),
						CreateSQL: oSql,
						RootPage:  uint(rp),
					})
				}
			}
			dx.Tables = tbls
			dx.Indexes = indxes
			dx.Triggers = trgs
			dx.Views = views
		}
	}

	return dx
}

// Get fetches a desriptive PRAGMA result from the database
// The cmdParam must already be listed. Use CmdParam()
// to select a cmd param. Example:
//
//	Get(gosqlite.CmdParam().CollationList)
func (d *DB) Get(cmdParam string) ([]map[string]any, error) {

	var err error

	list := fmt.Sprintf("%s", CmdParam())
	if !strings.Contains(list, cmdParam) {
		return nil, errors.New("invalid command")
	}

	var m []map[string]any
	qr := d.GetResultSet(fmt.Sprintf("PRAGMA %s;", db_cmd_get_database_list))
	err = qr.Err
	if err == nil {
		m = qr.ResultTable
	}

	return m, err
}

func (d *DB) GetBlob(tblName string, colName string, rowid int) ([]byte, error) {
	var b []byte

	return b, nil
}

// GetDBHandleUsers [linux only] gets the path of programs that
// are using the database.
func (d *DB) GetDBHandleUsers() ([]string, error) {

	var fUsers []string
	var out bytes.Buffer
	var pIDs []string

	if runtime.GOOS != "linux" {
		return fUsers, fmt.Errorf("this method is not yet implemented on %s", runtime.GOOS)
	}

	if d == nil {
		return fUsers, errors.New("database refernce is null")
	}

	cmdText := fmt.Sprintf("fuser %s", d.FilePath())
	cmd := exec.Command("bash", "-c", cmdText)
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return fUsers, err
	}

	fuser := out.String()
	fuser = strings.TrimSpace(strings.ReplaceAll(fuser, "  ", " "))
	v := strings.Split(fuser, " ")

	for i := 0; i < len(v); i++ {
		if v[i] == "" {
			continue
		}
		pIDs = append(pIDs, v[i])
	}

	for i := 0; i < len(pIDs); i++ {
		out.Reset()
		cmdText = fmt.Sprintf("ps -ax | grep %s", pIDs[i])
		cmd = exec.Command("bash", "-c", cmdText)
		cmd.Stdout = &out
		err = cmd.Run()
		if err != nil {
			return fUsers, err
		}
		s := out.String()
		s = strings.TrimSpace(strings.ReplaceAll(s, "  ", " "))
		v = strings.Split(s, " ")
		for j := 0; j < len(v); j++ {
			if strings.HasPrefix(v[j], "/") {
				fUsers = append(fUsers, v[j])
				break
			}
		}
	}

	return fUsers, nil
}

func (d *DB) GetPage(pageSize int, pageNo int, tableName string, filter string, orderBy string, sortOrder string) (DataTable, error) {
	var dt *DataTable

	sqly := fmt.Sprintf("select type from sqlite_master where lower([name]) = '%s';", strings.ToLower(tableName))
	tRes, _, _ := d.Prepare(sqly, []any{})
	C.sqlite3_step(tRes.cStmt)
	tblType := d.getStmtColVal(&tRes, 0)
	C.sqlite3_finalize(tRes.cStmt)

	sqlx := ""
	// prepare the sql statement
	if tblType != "view" {
		sqlx = fmt.Sprintf("select _rowid_,* from [%s]", tableName)
	} else {
		sqlx = fmt.Sprintf("select * from [%s]", tableName)
	}
	if filter != "" {
		filter = strings.ToLower(filter)
		filter = prunDoubleSpace(filter)
		filter = strings.TrimSpace(filter)
		filter = strings.TrimPrefix(filter, "where")
		filter = strings.TrimSpace(filter)
	}

	cols := d.getTableColumns(tableName)

	ci, err := d.GetPagingInfo(pageSize, pageNo, tableName, filter)
	if err != nil {
		if dt != nil {
			dt.Err = err
		}
		var dtx DataTable
		return dtx, err
	}

	order := ""
	if orderBy != "" {
		// see if the table exists
		colFnd := false
		for i := 0; i < len(cols); i++ {
			if strings.EqualFold(orderBy, cols[i].Name) {
				colFnd = true
				break
			}
		}
		if colFnd {
			if sortOrder == "" {
				sortOrder = "asc"
			}
			order = fmt.Sprintf("order by [%s] %s", orderBy, sortOrder)
		}
	}

	where := ""
	if filter != "" {
		where = "where ("
		for i := 0; i < len(cols); i++ {
			where = fmt.Sprintf("%s [%s] LIKE '%%%s%%' OR", where, cols[i].Name, filter)
		}
		where = strings.TrimSuffix(where, " OR") // remove the extra OR from the end
		where = fmt.Sprintf("%s)", where)        // add the closing parentises
	}

	offset := ci.PositionFrom
	if ci.PageNo == 1 {
		offset--
	}

	// enclose the statement inside () and select * from it
	sqlx = fmt.Sprintf("select * from (%s) %s %s limit %d offset %d", sqlx, where, order, ci.PageSize, offset)

	dt, err = d.GetDataTable(sqlx)
	if err != nil {
		if dt != nil {
			dt.Err = err
		}
		var dtx DataTable
		return dtx, err
	}

	if len(dt.Columns) == 0 {
		dt.Columns = cols
	}

	dt.CollInfo = ci

	return *dt, nil
}

// GetPageOffset returns totalPages, offset, pageNo
func (d *DB) GetPageOffset(recordCount int, pageSize int, pageNo int) (int, int, int) {

	if pageSize < 1 || recordCount < 1 {
		return 0, 0, 0
	}

	totalPages := recordCount / pageSize

	remainder := recordCount % pageSize
	if remainder > 0 {
		totalPages++
	}

	if pageNo > totalPages {
		pageNo = totalPages
	}

	offset := pageSize * (pageNo - 1)
	if offset < 0 {
		offset = 0
	}
	remainder = offset % pageSize
	if remainder > 0 {
		offset += pageSize
	}
	if offset < 0 {
		offset = 0
	}
	if offset > recordCount {
		offset = recordCount - pageSize
	}

	if pageNo < 1 {
		pageNo = 1
	}

	if totalPages < 1 {
		totalPages = 1
	}

	return totalPages, offset, pageNo
}

// GetPagingInfo returns the collectionInfo on a table
func (d *DB) GetPagingInfo(pageSize int, pageNo int, tableName string, filter string) (CollectionInfo, error) {

	var ci CollectionInfo

	if pageSize < 1 {
		pageSize = 50 // default is 50
	}

	if pageNo < 1 {
		pageNo = 1
	}

	sc := fmt.Sprintf("select count(*) from [%s]", tableName)

	if filter != "" {
		where := ""
		if filter != "" {
			where = "where ("
			// get all columns
			cols := d.getTableColumns(tableName)
			for i := 0; i < len(cols); i++ {
				where = fmt.Sprintf("%s [%s] LIKE '%%%s%%' OR", where, cols[i].Name, filter)
			}
			where = strings.TrimSuffix(where, " OR")
			where = fmt.Sprintf("%s)", where)
		}

		sc = fmt.Sprintf("select count(*) from [%s] %s", tableName, where)
	}

	recordCount := 0

	rObj, err := d.ExecuteScalare(sc)
	if err != nil {
		return ci, err
	}
	if rObj != nil {
		recordCount = int(rObj.(int64))
	}

	totalPages, offset, pageNo := d.GetPageOffset(recordCount, pageSize, pageNo)

	if pageNo > totalPages {
		pageNo = totalPages
	}

	ci.TotalPages = totalPages
	ci.PageNo = pageNo
	ci.PageSize = pageSize
	ci.RecordCount = recordCount
	ci.PositionFrom = offset //- pageSize
	ci.PositionTo = (offset + pageSize)

	if ci.PositionFrom < 1 {
		ci.PositionFrom = 1
	}

	if ci.PositionTo > recordCount {
		ci.PositionTo = recordCount
	}

	return ci, nil
}

// IsIdle reutrns true if a database is "busy" with
// onr (or more( of the following operations
// Exec(), GetDataTable(),  Execute(), ExececuteNonQuery(),
// or ExececuteScalre()
func (d *DB) IsIdle() bool {

	return ExecSeqNo < 1 &&
		GetDataTableSeqNo < 1 &&
		ExecuteSeqNo < 1 &&
		ExececuteNonQuerySeqNo < 1 &&
		ExececuteScalreSeqNo < 1 &&
		QuerySeqNo < 1
}

func (d *DB) IsInMemory() bool {
	return d.isInMemory
}

// UpdateBlob writes an array of []byte to a BLOB column.
// Note that BLOBs can also be updated via an sql statement
// as the following exmaple:
//
//	data := []byte("Hello World")
//	sqlSmt := fmt.Sprintf("UPDATE <table name> SET <column name> = x'%x'", data)
//
// also see: Update() in DataTable.
func (d *DB) UpdateBlob(data []byte, tblName string, colName string, rowid int) Result {

	var res Result

	if strings.Contains(tblName, " ") && !strings.HasPrefix(tblName, "[") && !strings.HasSuffix(tblName, "]") {
		tblName = fmt.Sprintf("[%s]", tblName)
	}

	ph := make([]any, 2)
	//ph[0] = b.Bytes() // compressed data
	ph[0] = data
	ph[1] = rowid

	query := fmt.Sprintf("update %s set %s = ? where _rowid_ = ?", tblName, colName)
	s, _, err := d.Prepare(query, ph)
	if err != nil {
		res.rowsAffected = -1
		res.err = err
		return res
	}
	rc := C.sqlite3_step(s.cStmt)
	if rc != SQLITE_DONE {
		err = getSQLiteErr(rc, d.DBHwnd)
		res.rowsAffected = -1
		res.err = err
	} else {
		res.rowsAffected = int64(C.sqlite3_total_changes(d.DBHwnd))
		res.lastInsertId = int64(C.sqlite3_last_insert_rowid(d.DBHwnd))
	}

	C.sqlite3_finalize(s.cStmt)

	return res
}

func (d *DB) ShrinkMemory() error {

	if d.Busy() {
		return nil
	}

	if d == nil || d.DBHwnd == nil {
		return nil
	}
	rc := C.sqlite3_db_release_memory(d.DBHwnd)

	return getSQLiteErr(rc, d.DBHwnd)
}

// SaveSchemaToFile saves a database schema to file.
// The database can in-memory or attached to a file.
func (d *DB) SaveSchemaToFile(schema string, dbFilePath string) error {

	if schema == "" {
		schema = "main"
	}

	dbFilePath = strings.ToLower(dbFilePath)
	if dbFilePath == "" {
		return errors.New("invalid file name")
	}

	if strings.ToLower(d.filePath) == strings.ToLower(dbFilePath) && !d.Closed {
		return errors.New("cannot save to an open database")
	}

	if fileOrDirExists(dbFilePath) {
		return errors.New("file already exists")
	}

	bDBContent, err := Serialize(d, schema)
	if err != nil {
		return err
	}
	err = DeserializeToFile(bDBContent, dbFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) Ping() int {
	if !d.Closed {
		return 0
	}
	return 1
}

func (d *DB) TxBegin() (string, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	restPoint := fmt.Sprintf("%x%x", b[0:4], b[4:6])
	sqlx := fmt.Sprintf(`SAVEPOINT "%s"`, restPoint)
	sqlxx := C.CString(sqlx)
	defer C.free(unsafe.Pointer(sqlxx))

	res := C.sqlite3_exec(d.DBHwnd, sqlxx, nil, nil, nil)
	err = getSQLiteErr(res, d.DBHwnd)
	if err != nil {
		return "", err
	}

	return restPoint, nil
}
func (d *DB) TxRollback(txID string) error {

	sqlx := fmt.Sprintf(`ROLLBACK TO SAVEPOINT  "%s";`, txID)
	sqlxx := C.CString(sqlx)
	defer C.free(unsafe.Pointer(sqlxx))

	res := C.sqlite3_exec(d.DBHwnd, sqlxx, nil, nil, nil)
	err := getSQLiteErr(res, d.DBHwnd)
	if err != nil {
		return err
	}

	sqlx = fmt.Sprintf(`RELEASE "%s";`, txID)
	sqlxx = C.CString(sqlx)
	res = C.sqlite3_exec(d.DBHwnd, sqlxx, nil, nil, nil)
	return getSQLiteErr(res, d.DBHwnd)
}
func (d *DB) TxCommit(txID string) error {
	sqlx := fmt.Sprintf(`RELEASE "%s";`, txID)
	sqlxx := C.CString(sqlx)
	defer C.free(unsafe.Pointer(sqlxx))

	res := C.sqlite3_exec(d.DBHwnd, sqlxx, nil, nil, nil)
	return getSQLiteErr(res, d.DBHwnd)
}
