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
	"path"
	"path/filepath"
	"strings"
	"time"
	"unsafe"
)

// CreateDatabase creates a new database with a
// default user table.
func CreateDatabase(dbFilePath string) error {

	if fileOrDirExists(dbFilePath) {
		return errors.New("db file already exists")
	}

	dir := filepath.Dir(dbFilePath)

	// create all sub-dirs
	if err := os.MkdirAll(dir, 0770); err != nil {
		return err
	}

	var db *C.sqlite3
	var rc C.int
	fName := C.CString(dbFilePath)
	defer C.free(unsafe.Pointer(fName))
	C.sqlite3_open(fName, &db)

	timenow := time.Now().String()
	sqlx := C.CString(
		fmt.Sprintf(
			`
		CREATE TABLE IF NOT EXISTS dbx_user (
			UserID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
			Name	        TEXT NOT NULL,
			UserName        TEXT NOT NULL,
			Email	        TEXT,
			Password        TEXT,
			Permissions     TEXT, /* possible values: r,w, or rw (read, write or read-write) */
			DateTimeCreated TEXT NOT NULL
		);
		CREATE UNIQUE INDEX INX_dbx_user_UserName ON dbx_user (UserName);
		CREATE INDEX INX_dbx_user_DateTimeCreated ON dbx_user (DateTimeCreated);
		insert into dbx_user
		select 1,'administrator','admin',NULL,'password','rw', '%s' 
		WHERE NOT EXISTS (SELECT 1 FROM dbx_user WHERE UserName='admin');`, timenow))

	rc = C.sqlite3_exec(db, sqlx, nil, nil, nil)
	C.free(unsafe.Pointer(sqlx))
	C.sqlite3_close(db)

	if int(rc) == SQLITE_OK {
		return nil
	}

	return errors.New(GetErrText(int(rc)))
}

func OpenV2Exclusive(dbFilePath string, pragma ...string) (*DB, error) {
	dhwnd, err := openV2(dbFilePath,
		C.SQLITE_OPEN_EXCLUSIVE|
			C.SQLITE_OPEN_READWRITE|
			C.SQLITE_OPEN_EXRESCODE|
			C.SQLITE_OPEN_FULLMUTEX, "", pragma)
	return dhwnd, err
}

// Open opens a read-only SQLite database and returns a pointer
// to DB. pragma is a list of PRAGMA commands to be applied before
// the database is ready for operation, typically journal-mode
// PRAGMAs.
func OpenV2Readonly(dbFilePath string, pragma ...string) (*DB, error) {

	dhwnd, err := openV2(dbFilePath,
		C.SQLITE_OPEN_READONLY|
			C.SQLITE_OPEN_EXRESCODE|
			C.SQLITE_OPEN_FULLMUTEX, "", pragma)

	return dhwnd, err
}

func OpenV2FullOption(dbFilePath string, vfsName string, flags C.int, pragma ...string) (*DB, error) {

	dhwnd, err := openV2(dbFilePath, flags, vfsName, pragma)

	return dhwnd, err
}

// Open opens an SQLite database and returns a pointer to DB.
// pragma is a list of PRAGMA command to be applied before
// the database is ready for operation, typically journal-mode
// PRAGMAs.
func OpenV2(dbFilePath string, pragma ...string) (*DB, error) {

	dhwnd, err := openV2(dbFilePath,
		C.SQLITE_OPEN_READWRITE|
			C.SQLITE_OPEN_EXRESCODE|
			C.SQLITE_OPEN_FULLMUTEX, "", pragma)

	return dhwnd, err
}

func OpenMemory(vfsName ...string) (*DB, error) {

	var vfsNamex string

	if len(vfsName) > 0 {
		vfsNamex = vfsName[0]
	} else {
		vfsNamex = "unix-excl"
	}

	d := initDB("")

	fMem := C.CString(":memory:?cache=shared")
	defer C.free(unsafe.Pointer(fMem))

	// see: https: //www.sqlite.org/vfs.html
	vfsNamePtr := C.CString(vfsNamex)
	defer C.free(unsafe.Pointer(vfsNamePtr))

	mCMutex.Lock()
	res := C.sqlite3_open_v2(fMem, &d.DBHwnd,
		C.SQLITE_OPEN_MEMORY|
			C.SQLITE_OPEN_READWRITE|
			C.SQLITE_OPEN_EXRESCODE|
			C.SQLITE_OPEN_FULLMUTEX,
		vfsNamePtr)

	err := getSQLiteErr(res, d.DBHwnd)
	mCMutex.Unlock()

	if err == nil {
		d.Closed = false

	} else {
		DBGrp.Add(&d)
	}

	return &d, err
}

// Open opens an SQLite database and returns a pointer to DB.
// pragma is a list of PRAGMA command to be applied before
// the database is ready for operation, typically journal-mode
// PRAGMAs. See https://sqlite.org/pragma.html#pragma_journal_mode.
func Open(dbFilePath string, pragma ...string) (*DB, error) {

	if !fileOrDirExists(dbFilePath) {
		return nil, errors.New("database file does not exists")
	}

	// see https://www.sqlite.org/pragma.html#pragma_secure_delete
	pragma = append(pragma, "PRAGMA main.secure_delete = ON")

	initGrouper()

	fPath := C.CString(dbFilePath)
	defer C.free(unsafe.Pointer(fPath))

	// init the db instance
	d := initDB(dbFilePath)

	mCMutex.Lock()
	res := C.sqlite3_open(fPath, &d.DBHwnd)
	err := getSQLiteErr(res, d.DBHwnd)
	mCMutex.Unlock()

	d.Name = strings.TrimSuffix(path.Base(dbFilePath), ".sqlite")

	if err == nil {
		d.Closed = false
	}
	for i := 0; i < len(pragma); i++ {
		_, err := d.Execute(pragma[i])
		if err != nil {
			return nil, err
		}
	}

	// refrence the db bak to its InMemory object,
	// otherwise its DBHwnd will be null
	d.InMemory.db = &d

	DBGrp.Add(&d)

	return &d, err
}

func GetVersion() SQLiteVersion {
	return SQLiteVersion{
		Version:       C.SQLITE_VERSION,
		VersionNumber: C.SQLITE_VERSION_NUMBER,
		SourceID:      C.SQLITE_SOURCE_ID}
}

func GetProcessID() int {
	return os.Getpid()
}

func DecryptDBFile(encFilePath string, desFile string, pwdPhrs string) error {

	encryptedBytes, err := os.ReadFile(encFilePath)
	if err != nil {
		return err
	}

	decryptedBytes, err := DecryptLight(encryptedBytes, pwdPhrs)
	if err != nil {
		return err
	}

	os.Remove(desFile)
	err = os.WriteFile(desFile, decryptedBytes, os.ModePerm)

	return err
}

// EncryptDBFile encrypts a database file.
func EncryptDBFile(dbPath string, encFilePath string, pwdPhrs string) error {

	// try this but don't return error
	db, err := Open(dbPath)
	if err == nil {
		db.Vacuum()
		db.Close()
	}

	bClear, err := os.ReadFile(dbPath)
	if err != nil {
		return err
	}
	bEncrypted, err := EncryptLight(bClear, pwdPhrs)
	if err != nil {
		return err
	}

	os.Remove(encFilePath)
	err = os.WriteFile(encFilePath, bEncrypted, os.ModePerm)

	return err
}

// openV2 opens an sqlite file with options.
// See:
// https://www.sqlite.org/inmemorydb.html
// https://www.sqlite.org/c3ref/open.html#urifilenameexamples
// https://www.sqlite.org/vfs.html
func openV2(dbFilePath string, flag C.int, vfsName string, pragma []string) (*DB, error) {

	if dbFilePath == "" || !fileOrDirExists(dbFilePath) {
		return nil, errors.New("database file does not exist")
	}

	// see https://www.sqlite.org/pragma.html#pragma_secure_delete
	pragma = append(pragma, "PRAGMA main.secure_delete = ON")

	initGrouper()

	if vfsName == "" {
		vfsName = "unix-excl"
	}

	d := initDB(dbFilePath)

	fPath := C.CString(dbFilePath)
	defer C.free(unsafe.Pointer(fPath))

	// see: https: //www.sqlite.org/vfs.html
	vfsNamePtr := C.CString(vfsName)
	defer C.free(unsafe.Pointer(vfsNamePtr))

	mCMutex.Lock()
	res := C.sqlite3_open_v2(fPath, &d.DBHwnd, flag, vfsNamePtr)
	err := getSQLiteErr(res, d.DBHwnd)
	mCMutex.Unlock()

	if err == nil {
		d.Closed = false
	}

	// see: https://sqlite.org/pragma.html#pragma_journal_mode
	for i := 0; i < len(pragma); i++ {
		_, err := d.Execute(pragma[i])
		if err != nil {
			return nil, err
		}
	}

	DBGrp.Add(&d)

	return &d, err
}

// CreateGroup creates a new directory on the file system.
// Group is actually a directory in which db files reside in.
func CreateGroup(dirPath string) error {

	if err := os.MkdirAll(dirPath, 0770); err != nil {
		return err
	}

	return nil
}

// setColumnType sets the column type according to that
// of its value. see: see: https://www.sqlite.org/c3ref/column_decltype.html:
// "SQLite uses dynamic run-time typing. So just because a column is declared
// to contain a particular type does not mean that the data stored in that column
// is of the declared type. SQLite is strongly typed, but the typing is dynamic
// not static. Type is associated with individual values, not with the containers
// used to hold those values."
//
// also see: see: https://www.sqlite.org/datatype3.html
func GetSQLiteDataType(fieldValue any) string {

	dataType := ""

	t := fmt.Sprintf("%T", fieldValue)

	switch t {
	case "<nil>":
		dataType = SQLiteDataType().NULL

	case "int":
		dataType = SQLiteDataType().INTEGER

	case "string":
		dataType = SQLiteDataType().TEXT

	case "int64":
		dataType = SQLiteDataType().INTEGER

	case "[]uint8":
		dataType = SQLiteDataType().BLOB

	case "float64":
		dataType = SQLiteDataType().REAL

	default:
		dataType = SQLiteDataType().VARIANT
	}

	return dataType
}

func GetErrText(errN int) string {

	var errTxt string

	switch errN {
	case SQLITE_OK:
		errTxt = "successful result"
	case SQLITE_ERROR:
		errTxt = "Generic error"
	case SQLITE_INTERNAL:
		errTxt = "internal logic error in SQLite"
	case SQLITE_PERM:
		errTxt = "access permission denied"
	case SQLITE_ABORT:
		errTxt = "callback routine requested an abort"
	case SQLITE_BUSY:
		errTxt = "database file is locked"
	case SQLITE_LOCKED:
		errTxt = "database table is locked"
	case SQLITE_NOMEM:
		errTxt = "out of memory"
	case SQLITE_READONLY:
		errTxt = "attempt to write a readonly database"
	case SQLITE_INTERRUPT:
		errTxt = "interrupted"
	case SQLITE_IOERR:
		errTxt = "disk I/O error error"
	case SQLITE_CORRUPT:
		errTxt = "database disk image is malformed"
	case SQLITE_NOTFOUND:
		errTxt = "unknown operation"
	case SQLITE_FULL:
		errTxt = "database or disk is full"
	case SQLITE_CANTOPEN:
		errTxt = "unable to open database file"
	case SQLITE_PROTOCOL:
		errTxt = "locking protocol error"
	case SQLITE_EMPTY:
		errTxt = "internal use only"
	case SQLITE_SCHEMA:
		errTxt = "database schema has changed"
	case SQLITE_TOOBIG:
		errTxt = "string or blob too big"
	case SQLITE_CONSTRAINT:
		errTxt = "constraint failed"
	case SQLITE_MISMATCH:
		errTxt = "database schema has changed"
	case SQLITE_MISUSE:
		errTxt = "bad parameter or other API misuse"
	case SQLITE_NOLFS:
		errTxt = "large file support is disabled"
	case SQLITE_AUTH:
		errTxt = "authorization denied"
	case SQLITE_FORMAT:
		errTxt = "not used"
	case SQLITE_RANGE:
		errTxt = "column index out of range"
	case SQLITE_NOTADB:
		errTxt = "File opened that is not a database file"
	case SQLITE_NOTICE:
		errTxt = "notification message"
	case SQLITE_WARNING:
		errTxt = "Warning message"
	case SQLITE_ROW:
		errTxt = "sqlite3_step() has another row ready"
	case SQLITE_DONE:
		errTxt = "sqlite3_step() has finished executing"
	default:
		errTxt = "unknown error"
	}

	return errTxt
}

// --------- other public ----------
func GetTableNameFromSQLQuery(sqlQuery string) string {
	sqlQueryLower := strings.ToLower(sqlQuery)
	x := strings.Split(sqlQueryLower, " ")

	y := make([]interface{}, len(x))
	for i, v := range x {
		y[i] = v
	}

	vx := removeElmFrmArry(y, "")

	count := len(vx)
	for i := 0; i < count; i++ {
		vxstr := vx[i].(string)
		if vxstr == "from" || vxstr == "into" {
			if (i + 1) < count {
				v := vx[i+1].(string)
				if strings.HasSuffix(v, ";") {
					v = v[:len(v)-1]
				}

				tblName := strings.TrimSpace(strings.Split(v, "\n")[0])
				return tblName
			}
		}
	}
	return ""
}

// TODO: Compress/depress data before/after writing to the database
//       on-demand?
//
// func CompressBytes(b []byte) ([]byte, error) {

// 	var buf bytes.Buffer
// 	gw, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
// 	if err != nil {
// 		return nil, err
// 	}
// 	gw.Write(b)
// 	gw.Close()

// 	return buf.Bytes(), nil
// }

// func DecompressBytes(b []byte) ([]byte, error) {

// 	buf := bytes.NewBuffer(b)
// 	gzipReader, err := gzip.NewReader(buf)
// 	if err != nil {
// 		return nil, err
// 	}

// 	r := make([]byte, len(buf.Bytes()))
// 	gzipReader.Read(r)
// 	gzipReader.Close()

// 	return r, nil
// }
