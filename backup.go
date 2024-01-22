// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

// #include <stdio.h>
// #include <stdlib.h>
// #include "sqlite3.h"
// void go_xProgress(int x, int y, sqlite3 *db, char *extraData);
// void funcProgress(int x, int y, sqlite3 *db , char *extraData){
// 	go_xProgress(x, y, db, extraData);
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

// BackupOnlineDB takes a backup of an online database. It's done
// page-by-page. The progress is written to a callback.
// see: https://www.sqlite.org/backup.html.
// Note that filePathBackupTo must alrady exist; use CreateDatabase()
// to create an empty database, if file does not exist.
// millSecToSleepBeforeRepeating can be set to zero (with no delays),
// although the recommanded delay time is 250 milliseconds.
func BackupOnlineDB(
	onlineDB *DB, /* Pointer to an open database to back up */
	filePathBackupTo string, /* Full path of the file to back up to */
	millSecToSleepBeforeRepeating int, /* wait-time between copying pages */
	prgCallback func(xPagesCopied int, yTotalPages int, data string), /* Progress function to invoke */
	extraData string, /* extra data to send to callback */
	options ...string /*backup_raise_err_on_busy ir backup_raise_err_on_dblocked*/) error {

	if !fileOrDirExists(filePathBackupTo) {
		return errors.New("database file does not exist")
	}

	if millSecToSleepBeforeRepeating < 0 {
		millSecToSleepBeforeRepeating = 0
	}

	// options
	errOnBusy := false
	errOnDBLocked := false
	for i := 0; i < len(options); i++ {
		if options[i] == backup_raise_err_on_dblocked {
			errOnDBLocked = true

		} else if options[i] == backup_raise_err_on_busy {
			errOnBusy = true
		}
	}

	onlineDB.BackupProgress = prgCallback

	extraDataC := C.CString(extraData)

	zFilename := C.CString(filePathBackupTo)
	defer C.free(unsafe.Pointer(zFilename))

	var pFile *C.sqlite3          // Database connection opened on zFilename
	var pBackup *C.sqlite3_backup // Backup handle used to copy data

	// Open the database file identified by zFilename.
	rc := C.sqlite3_open(zFilename, &pFile)
	if rc == C.SQLITE_OK {

		mainTxt := C.CString("main")
		defer C.free(unsafe.Pointer(mainTxt))

		// Open the sqlite3_backup object used to accomplish the transfer.
		pBackup = C.sqlite3_backup_init(pFile, mainTxt, onlineDB.DBHwnd, mainTxt)
		if pBackup != nil {

			// Each iteration of this loop copies 5 database pages from database
			// pDb to the backup database. If the return value of backup_step()
			// indicates that there are still further pages to copy, sleep for
			// n ms before repeating.
			for {
				rc = C.sqlite3_backup_step(pBackup, 5)

				if errOnBusy && rc == C.SQLITE_BUSY {
					break
				}

				if errOnDBLocked && rc == C.SQLITE_LOCKED {
					break
				}

				if rc != C.SQLITE_OK && rc != C.SQLITE_BUSY && rc != C.SQLITE_LOCKED {
					break
				}

				C.funcProgress(C.sqlite3_backup_remaining(pBackup),
					C.sqlite3_backup_pagecount(pBackup), onlineDB.DBHwnd, extraDataC)

				if rc == C.SQLITE_OK || rc == C.SQLITE_BUSY || rc == C.SQLITE_LOCKED {
					C.sqlite3_sleep(C.int(millSecToSleepBeforeRepeating))
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
// of a database file. This is usually array of bytes read from a
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
	** for any reason. */
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
		time.Sleep(250 * time.Millisecond)
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
