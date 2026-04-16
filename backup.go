// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

package gosqlite

// #include <stdio.h>
// #include <stdlib.h>
// #include "sqlite3.h"
// void go_xProgress(int x, int y, sqlite3 *db, char *err, char *extraData);
// void funcProgress(int x, int y, sqlite3 *db , char *err, char *extraData){
// 	go_xProgress(x, y, db, err, extraData);
//}
import "C"
import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
	"unsafe"
)

// LoadOrSaveDb loads or saves in-memory database.
// The following description is from https://sqlite.org/backup.html:
// This function is used to load the contents of a database file on disk
// into the "main" database of open database connection pInMemory, or
// to save the current contents of the database opened by pInMemory into
// a database file on disk. pInMemory is probably an in-memory database,
// but this function will also work fine if it is not.
//
// Parameter zFilename points to a nul-terminated string containing the
// name of the database file on disk to load from or save to. If parameter
// isSave is non-zero, then the contents of the file zFilename are
// overwritten with the contents of the database opened by pInMemory. If
// parameter isSave is zero, then the contents of the database opened by
// pInMemory are replaced by data loaded from the file zFilename.
//
// If the operation is successful, SQLITE_OK is returned. Otherwise, if
// an error occurs, an SQLite error code is returned.
func LoadOrSaveDb(pMemDB *DB, zFilename string, isSave bool) error {
	// Open the database file identified by zFilename.
	fPath := C.CString(zFilename)
	defer C.free(unsafe.Pointer(fPath))

	pFileDB := initDB(zFilename)

	rc := C.sqlite3_open(fPath, &pFileDB.DBHwnd)

	err := getSQLiteErr(rc, pFileDB.DBHwnd)
	if err != nil {
		return err
	}

	var pFrom *C.sqlite3
	var pTo *C.sqlite3

	/* If this is a 'load' operation (isSave==0), then data is copied
	from the database file just opened to database pInMemory.
	Otherwise, if this is a 'save' operation (isSave==1), then data
	is copied from pInMemory to pFile.  Set the variables pFrom and
	pTo accordingly. */
	if !isSave {
		// load from file to memory
		pFrom = pFileDB.DBHwnd
		pTo = pMemDB.DBHwnd
	} else {
		// save from memory to file
		pFrom = pMemDB.DBHwnd
		pTo = pFileDB.DBHwnd
	}

	/* Set up the backup procedure to copy from the "main" database of
	connection pFile to the main database of connection pInMemory.
	If something goes wrong, pBackup will be set to NULL and an error
	code and message left in connection pTo.
	**
	If the backup object is successfully created, call backup_step()
	to copy data from pFile to pInMemory. Then call backup_finish()
	to release resources associated with the pBackup object.  If an
	error occurred, then an error code and message will be left in
	connection pTo. If no error occurred, then the error code belonging
	to pTo is set to SQLITE_OK.
	*/
	pBackup := C.sqlite3_backup_init(pTo, C.CString("main"), pFrom, C.CString("main"))
	if pBackup != nil {
		C.sqlite3_backup_step(pBackup, -1)
		C.sqlite3_backup_finish(pBackup)
	}

	rc = C.sqlite3_errcode(pTo)

	C.sqlite3_close(pFileDB.DBHwnd)

	if int(rc) == 0 {
		return nil // successful result
	}
	errTxt := GetErrText(int(rc))
	err = errors.New(errTxt)

	return err
}

// BackupOnlineDB takes a backup of an online database. It's done
// page-by-page. The progress is written to a callback.
// see: https://www.sqlite.org/backup.html.
// Note that filePathBackupTo must already exist.
// millSecToSleepBeforeRepeating can be set to zero (with no delays),
// although the recommanded delay time is 250 milliseconds.
func BackupOnlineDB(
	onlineDB *DB, /* Pointer to an open database to back up */
	filePathBackupTo string, /* Full path of the file to back up to */
	nPages int, /* number of pages to copy at a time */
	millSecToSleepBeforeRepeating int, /* wait-time between copying pages */
	prgCallback func(xPagesCopied int, yTotalPages int, err string, data string), /* Progress function to invoke */
	extraData string, /* extra data to send to callback */
	options ...string /*backup_raise_err_on_busy or backup_raise_err_on_dblocked*/) error {

	if !fileOrDirExists(filePathBackupTo) {
		err := CreateDatabase(filePathBackupTo)
		if err != nil {
			return err
		}
	}

	if onlineDB.DBHwnd == nil {
		return errors.New("database is offline")
	}

	if millSecToSleepBeforeRepeating < 0 {
		millSecToSleepBeforeRepeating = 0
	}

	// options
	errOnBusy := false
	errOnDBLocked := false
	noLoop := false
	for i := range options {
		switch options[i] {
		case BackupRaiseErrOnDBLocked:
			errOnDBLocked = true
		case BackupNoLoop:
			noLoop = true
		case BackupRaiseErrOnBusy:
			errOnBusy = true
		}
	}

	// override other errors,if noLoop option is on.
	// noLoop, means that the very first cycle
	// must be compoleted; so only exit loop if the database
	// is not busy or locked
	if noLoop {
		errOnBusy = false
		errOnDBLocked = false
	}

	onlineDB.BackupProgress = prgCallback

	znPages := C.int(nPages)

	extraDataC := C.CString(extraData)
	defer C.free(unsafe.Pointer(extraDataC))

	zFilename := C.CString(filePathBackupTo)
	defer C.free(unsafe.Pointer(zFilename))

	var pFile *C.sqlite3          // Database connection opened on zFilename
	var pBackup *C.sqlite3_backup // Backup handle used to copy data

	dbx, _ := OpenV2(filePathBackupTo, "PRAGMA main.journal_mode = TRUNCATE")
	dbx.Close()

	// Open the database file identified by zFilename.
	rc := C.sqlite3_open(zFilename, &pFile)
	if rc == C.SQLITE_OK {

		mainTxt := C.CString("main")
		defer C.free(unsafe.Pointer(mainTxt))

		if onlineDB.DBHwnd == nil {
			return errors.New("nil pointer to source database")
		}

		// Open the sqlite3_backup object used to accomplish the transfer.
		pBackup = C.sqlite3_backup_init(pFile, mainTxt, onlineDB.DBHwnd, mainTxt)
		if pBackup != nil {

			// Each iteration of this loop copies n database pages from database
			// pDb to the backup database. If the return value of backup_step()
			// indicates that there are still further pages to copy, sleep for
			// n ms before repeating.
			for {
				rc = C.sqlite3_backup_step(pBackup, znPages)

				if errOnBusy && rc == C.SQLITE_BUSY {
					break
				}

				if errOnDBLocked && rc == C.SQLITE_LOCKED {
					break
				}

				if rc != C.SQLITE_OK && rc != C.SQLITE_BUSY && rc != C.SQLITE_LOCKED {
					break
				}

				bkPgCnt := C.sqlite3_backup_pagecount(pBackup)
				remPgCnt := C.sqlite3_backup_remaining(pBackup)
				errTxt := C.CString(GetErrText(int(rc)))

				if rc == C.SQLITE_OK {
					errTxt = C.CString("")
				}

				if bkPgCnt == 0 && remPgCnt == 0 {
					// most likely the database is lockked or busy;
					// and it would possile that it will be locked/busy for
					// a long time:
					if millSecToSleepBeforeRepeating == 0 {
						// add some sleep time s that the process
						// is not halted
						C.sqlite3_sleep(100)
					}
				}

				C.funcProgress(remPgCnt, bkPgCnt, onlineDB.DBHwnd, errTxt, extraDataC)

				if rc == C.SQLITE_OK || rc == C.SQLITE_BUSY || rc == C.SQLITE_LOCKED {
					C.sqlite3_sleep(C.int(millSecToSleepBeforeRepeating))
				}

				// C.SQLITE_OK means page backedup ok and there is more to back up
				if noLoop && bkPgCnt > 0 && rc == C.SQLITE_OK && rc != C.SQLITE_BUSY && rc != C.SQLITE_LOCKED {
					break
				}
			}

			// Release resources allocated by backup_init().
			C.sqlite3_backup_finish(pBackup)
		}
		rc = C.sqlite3_errcode(pFile)
	}

	// Close the database connection opened on database
	// that data backed-up-to (file zFilename) and return the result of this function.
	C.sqlite3_close(pFile)

	if int(rc) == 0 {
		return nil // successful result
	}

	errTxt := GetErrText(int(rc))
	err := errors.New(errTxt)

	return err
}

// RepairSqlite3FileStub replaces the sqlite3 stub of a db-file.
func RepairSqlite3FileStub(bDBContent []byte) []byte {

	// get the empty header and replace the first 32 bytes.
	stubB, _ := hex.DecodeString(sqlite3_dbfile_emtpy_header)
	outB := stubB
	for i := 0; i < len(bDBContent); i++ {
		if i < len(stubB) {
			continue
		}
		outB = append(outB, bDBContent[i])
	}
	bDBContent = outB

	return bDBContent
}

// RemoveFileHeaderFromFileContent removes a db-file's
// file header; making it unreadable for most SQLite tools.
func RemoveFileHeaderFromFileContent(bDBContent []byte) []byte {

	bDBContent = bDBContent[32:]

	return bDBContent
}

// AddEmptyHeaderToFileContent adds the the sqlite3
// file header to a db-file.
func AddEmptyHeaderToFileContent(bDBContent []byte) []byte {

	if !IsFileSQLiteFormat(bDBContent) {
		stubB, _ := hex.DecodeString(sqlite3_dbfile_emtpy_header)
		outB := stubB

		for i := 0; i < len(bDBContent); i++ {
			outB = append(outB, bDBContent[i])
		}
		bDBContent = outB
	}

	return bDBContent
}

// DeserializeToFile creates an SQLite database file from a byte array.
func DeserializeToFile(bDBContent []byte, dbFilePath string) error {

	bDBContent = RepairSqlite3FileStub(bDBContent)

	err := os.WriteFile(dbFilePath, bDBContent, os.ModePerm)
	if err != nil {
		return err
	}
	// make sure open/close works
	db, err := Open(dbFilePath)
	if err != nil {
		return err
	}
	err = db.Close()
	if err != nil {
		return err
	}

	return nil
}

// DeserializeToInMemoryDB opens an in-memory database from []bytes
// of a database file. This is an array of bytes read from a
// database on file.
func DeserializeToInMemoryDB(bDBContent []byte, schema string) (*DB, error) {

	var zSchema *C.char
	var pFile *C.sqlite3          /* Database connection opened on zFilename */
	var pBackup *C.sqlite3_backup /* Backup object used to copy data */
	var pTo *C.sqlite3            /* Database to copy to (pFile or pInMemory) */
	var pFrom *C.sqlite3          /* Database to copy from (pFile or pInMemory) */

	if schema == "" {
		schema = "main"
	}
	zSchema = C.CString(schema)
	defer C.free(unsafe.Pointer(zSchema))

	dmMem, _ := OpenMemory()
	pTo = dmMem.DBHwnd

	var tmpFile string
	var tmpDir string
	fn := createHash(time.Now().String())

	if runtime.GOOS == "windows" {
		tmpDir = os.Getenv("temp")
	} else {
		tmpDir = "/var/tmp"
	}
	// just in case
	os.Mkdir(tmpDir, os.ModePerm)

	if runtime.GOOS == "windows" {
		tmpFile = fmt.Sprintf("%s\\%s.sqlite", os.Getenv("temp"), fn)
	} else {
		tmpFile = fmt.Sprintf("%s/%s.sqlite", tmpDir, fn)
	}

	// remove the journal files, if they're left behind.
	fnoext := strings.TrimSuffix(tmpFile, ".sqlite")
	walFile := fmt.Sprintf("%s.sqlite-wal", fnoext)
	shmFile := fmt.Sprintf("%s.sqlite-shm", fnoext)

	// if exist
	os.Remove(tmpFile)
	os.Remove(walFile)
	os.Remove(shmFile)

	os.WriteFile(tmpFile, bDBContent, os.ModePerm)

	zFilename := C.CString(tmpFile)
	defer C.free(unsafe.Pointer(zFilename))

	/* Open the database file identified by zFilename. Exit early if this fails
	for any reason. */
	rc := C.sqlite3_open(zFilename, &pFile)
	if rc == C.SQLITE_OK {

		pFrom = pFile
		pTo = dmMem.DBHwnd

		pBackup = C.sqlite3_backup_init(pTo, zSchema, pFrom, zSchema)
		if pBackup != nil {
			C.sqlite3_backup_step(pBackup, -1)
			C.sqlite3_backup_finish(pBackup)
		}
		rc = C.sqlite3_errcode(pTo)
	}

	C.sqlite3_close(pFile)

	go func() {
		// time.Sleep(250 * time.Millisecond)
		C.sqlite3_sleep(250)
		os.Remove(tmpFile)
		os.Remove(walFile)
		os.Remove(shmFile)
	}()

	if int(rc) == 0 {
		return dmMem, nil
	}

	err := errors.New(GetErrText(int(rc)))

	return nil, err
}

// IsFileSQLiteFormat reads the first bytes of an sqlite3
// database file...if those bytes begin with "SQLite format 3",
// then the db file was created by the sqlite3_open() function.
// Note that the database may still work without this string.
func IsFileSQLiteFormat(bDBContent []byte) bool {
	bsxLen := len("SQLite format 3")
	if len(bDBContent) < bsxLen {
		return false
	}

	var bsx = make([]byte, bsxLen)
	for i := 0; i < bsxLen; i++ {
		bsx[i] = bDBContent[i]
	}
	if strings.HasPrefix(string(bsx), "SQLite format 3") {
		return true
	}

	return false
}

// SaveInMemoryDBToFile backs up a schema into a database file.
// See: sqlite3_dbfile_stub.
func SaveInMemoryDBToFile(pInMemory *DB, schema string, dbFilePath string) error {

	if fileOrDirExists(dbFilePath) {
		return errors.New("file already exists")
	}

	bDBContent, err := Serialize(pInMemory, schema)
	if err != nil {
		return err
	}
	err = DeserializeToFile(bDBContent, dbFilePath)
	if err != nil {
		return err
	}

	return nil
}

// Serialize saves an opened database as array of []bytes.
func Serialize(db *DB, schema string) ([]byte, error) {

	if schema == "" {
		schema = "main" // default
	}

	var piSize C.sqlite3_int64
	var zSchema *C.char
	var bDBContent []byte

	zSchema = C.CString(schema)
	defer C.free(unsafe.Pointer(zSchema))

	ptr := C.sqlite3_serialize(db.DBHwnd, zSchema, &piSize, 0)
	if ptr == nil {
		return nil, errors.New("serialization failed")
	}
	defer C.sqlite3_free(unsafe.Pointer(ptr))

	bDBContent = unsafe.Slice((*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(ptr)))), int64(piSize))
	if bDBContent == nil {
		return nil, errors.New("serialization failed")
	}

	return bDBContent, nil
}
