// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

// #include "sqlite3.h"
import "C"
import (
	"sync"
	"time"
)

var mResultQueue []queryResult
var mCMutex sync.Mutex // global mutex
var DBGrp IDBGroup = &DBGroup{}

// GetResultSetSeqNo is incremented every time a request comes in
// for GetDataTable. It keep track of the order of the request.
// It is useful to track requests; it is also returned to the caller.
// This is sepcially helpful when the caller is using multiple threads
// where result are returned out-of-order (not by the order the request.
var GetResultSetSeqNo uint

// maxConcurrentRequest is a global counter, of all connections
// (single and multi-threaded) for the DateDateTable(). It helps
// keep track of freqeuent calls, and also helps in minimizing null-
// pointer situations where releasing resources could go out-of-synce
// between sqlite3 and that of Go. The caller can also use this counter
// to keep track of its results when calls are made concurrently.
// The following is an example of how a caller can create frequent/
// multiple threads.
//
//	for i:=0; i < n; i++{
//	  go db.GetDataTable("<some query with a fairly large result-set")
//	}
var GetDataTableSeqNo int
var ExecSeqNo int
var ExecuteSeqNo int
var ExececuteNonQuerySeqNo int
var ExececuteScalreSeqNo int
var QuerySeqNo int

type Value any

type Stmt struct {
	Query    string
	cStmt    *C.sqlite3_stmt
	released bool
}

type Rows struct {
	stmt     *Stmt
	db       *DB
	colCount int
	intfc    IRows // this makes sure IRows is implemented
}

type SQLiteVersion struct {
	Version       string
	VersionNumber uint
	SourceID      string
}

const (
	NON_QUERY  uint8 = 0
	DATA_TABLE uint8 = 1
)
const (
	reqverb_sqlite3 string = "sqlite3Request"
)

// QueryResult is used by the Exec callbck to
// return query-result to the caller.
type QueryResult struct {
	// SeqNo is the order of the request recieved
	// by the database instance.
	SeqNo uint

	// ResultTable is table of query results.
	ResultTable []map[string]any

	Columns []Column

	// QueryID is a unique id assiged to
	// the query. TODO: this id can be used to track a query
	// transaction logs; once implemented.
	QueryID string

	// Err is any error recevied from the
	// exec func.
	Err error

	// TimeStarted is the time the query started.
	TimeStarted time.Time

	// TimeEnded is the time query completed.
	TimeEnded time.Time
}

// queryResult is for internal processing
// of requests in a queue.
type queryResult struct {
	SeqNo    uint
	Result   []map[string]any
	QueryID  string
	Started  bool
	Finished bool
	Err      error

	// ExecResult is the results returned by the C code.
	// 0 means no error.
	ExecResult int
	Received   bool
	Processed  bool

	TimeStarted time.Time
	TimeEnded   time.Time

	// AttemptsAfterDBLocked is count of attemps to write.
	// after a "database locked" error. If the database is
	// locked, there will be three more attempts to try with
	// the same query. If the database-lock is not cleared after the
	// 3rd attemp, a database locked error will be
	// returned to the caller.
	AttemptsAfterDBLocked int
	SQLStmt               string
	QueryType             uint8
}
type DBStat struct {
	Size            string // i.e. 1,350KB, 2,535MB, 1.45GB
	Name            string
	FilePath        string
	FilePathHidden  bool
	DateTimeCreated time.Time
	LastModified    time.Time
	Tables          []Table
	Indexes         []Index
	Views           []View
	Triggers        []Trigger
}
type Column struct {
	Name              string `json:"Name"`
	DataType          string `json:"DataType"`
	IsPrimaryKey      bool   `json:"IsPrimaryKey"`
	IsAutoIncrement   bool   `json:"IsAutoIncrement"`
	IsGeneratedAlways bool   `json:"IsGeneratedAlways"`
	NotNULL           bool   `json:"NotNULL"`
	Ordinal           int    `json:"Ordinal"`
	DefaultValue      any    `json:"DefaultValue"`
}
type AttachedDB struct {
	Name       string `json:"Name"`
	DBName     string `json:"DBName"`
	DBFilePath string `json:"DBFilePath"`
}
type DataTable struct {
	Name    string   `json:"name"`
	Columns []Column `json:"column"`
	Rows    DataRow  `json:"data-row"`

	// db is the database the DataTable
	// is connected to.
	db *DB

	// SQLTail is the end of parsed string
	// (unused portion of the sql statement)
	SQLTail string `json:"sql-tail"`

	// SeqNo is a global counter for the GetDataTable().
	// It can be used to track/verify multi-treaded calls.
	SeqNo int `json:"seq-no"`

	// QueryID  string
	TimeStarted time.Time `json:"time-started"`
	TimeEnded   time.Time `json:"time-ended"`

	// Err is the error recieved at the time
	// the data is fetched.
	Err error `json:"err"`

	CollInfo CollectionInfo
}

type CollectionInfo struct {
	RecordCount  int
	TotalPages   int
	PageSize     int
	PageNo       int
	PositionFrom int
	PositionTo   int
}

type DataRow []map[string]any

type IDataTable interface {

	// NewRow returns an empty row.
	NewRow() DataRow

	// InsertRow inserts row into table.
	InsertRow(row DataRow) error

	// Clears the table; it does not remove data
	// from database; only from the cache.
	Clear()

	DescribeDatabase() DBStat

	// Update upates rows according to rows/columns
	// in the table.
	Update() error

	ExportToCSV(destFilePath string) error
}

// goChanWork is a basic type to pass
// to a go routine channel.
type goChanWork struct {
	returnObj any
	err       error
}

type Table struct {
	Name      string
	Columns   []Column
	CreateSQL string
	RootPage  uint
}
type Index struct {
	Name      string
	CreateSQL string
	Unique    bool
	RootPage  uint
	Columns   []string
}
type View struct {
	Name      string
	RootPage  uint
	CreateSQL string
}
type Trigger struct {
	Name      string
	RootPage  uint
	CreateSQL string
}

type ConnectionPool struct {
	MaxOpenConns          int           // 0 means unlimited
	MaxIdleConns          int           // 0 means unlimited
	MaxLifetime           time.Duration // maximum amount of time a connection may be reused
	MaxIdleTime           time.Duration // maximum amount of time a connection may be idle before being closed
	MaxConcurrentRequests int
}

type Connection struct {
	TimeConnected time.Time
	ID            string
	SeqNo         uint
}

type DB struct {
	// DBHwnd is a pointer to the sqlite3 structure.
	DBHwnd *C.sqlite3

	filePath      string // full path of the database file.
	daemonStarted bool
	isInMemory    bool // whether the db is created in memory

	// not unique; e.g. name can be used to get a list of
	// databse by name. See IDBGroup.Get()
	Name        string
	JournalMode string
	UniqueName  string // hash of the db file path
	ConnPool    ConnectionPool
	Closed      bool
	ConnString  string

	// InMemory are objects created in-memory regarless of how
	// a database was opened. e.g. a database opened via a vile
	// can create in-memory tables.
	// See PRAGMA temp_store on https://www.sqlite.org/pragma.html
	InMemory InMemoryObjects

	BackupProgress func(xPagesCopied int, yTotalPages int, data string)

	Connections []Connection

	TimeOpened time.Time

	getResultSetConnections uint
	intfce                  IDB // this makes sure IDB is implemented
}

// sqlite3_dbfile_emtpy_header is a hex of the first 32 bytes of an sqlite3
// emtpy database file (no tables). It's part of the first 100 bytes
// of the file header.
// See: DeserializeToFile() and FORMAT DETAILS in sqlite3.c
// for a complete description.
const sqlite3_dbfile_emtpy_header = "53514c69746520666f726d6174203300100002020040202000017e1e00000009"

const (
	SQLITE_INTEGER = 1
	SQLITE_FLOAT   = 2
	SQLITE_BLOB    = 4
	SQLITE_NULL    = 5
	SQLITE_TEXT    = 3
	SQLITE3_TEXT   = 3
)

// error messages
const (
	SQLITE_OK         = 0   /* Successful result */
	SQLITE_ERROR      = 1   /* Generic error */
	SQLITE_INTERNAL   = 2   /* Internal logic error in SQLite */
	SQLITE_PERM       = 3   /* Access permission denied */
	SQLITE_ABORT      = 4   /* Callback routine requested an abort */
	SQLITE_BUSY       = 5   /* The database file is locked */
	SQLITE_LOCKED     = 6   /* A table in the database is locked */
	SQLITE_NOMEM      = 7   /* A malloc() failed */
	SQLITE_READONLY   = 8   /* Attempt to write a readonly database */
	SQLITE_INTERRUPT  = 9   /* Operation terminated by sqlite3_interrupt()*/
	SQLITE_IOERR      = 10  /* Some kind of disk I/O error occurred */
	SQLITE_CORRUPT    = 11  /* The database disk image is malformed */
	SQLITE_NOTFOUND   = 12  /* Unknown opcode in sqlite3_file_control() */
	SQLITE_FULL       = 13  /* Insertion failed because database is full */
	SQLITE_CANTOPEN   = 14  /* Unable to open the database file */
	SQLITE_PROTOCOL   = 15  /* Database lock protocol error */
	SQLITE_EMPTY      = 16  /* Internal use only */
	SQLITE_SCHEMA     = 17  /* The database schema changed */
	SQLITE_TOOBIG     = 18  /* String or BLOB exceeds size limit */
	SQLITE_CONSTRAINT = 19  /* Abort due to constraint violation */
	SQLITE_MISMATCH   = 20  /* Data type mismatch */
	SQLITE_MISUSE     = 21  /* Library used incorrectly */
	SQLITE_NOLFS      = 22  /* Uses OS features not supported on host */
	SQLITE_AUTH       = 23  /* Authorization denied */
	SQLITE_FORMAT     = 24  /* Not used */
	SQLITE_RANGE      = 25  /* 2nd parameter to sqlite3_bind out of range */
	SQLITE_NOTADB     = 26  /* File opened that is not a database file */
	SQLITE_NOTICE     = 27  /* Notifications from sqlite3_log() */
	SQLITE_WARNING    = 28  /* Warnings from sqlite3_log() */
	SQLITE_ROW        = 100 /* sqlite3_step() has another row ready */
	SQLITE_DONE       = 101 /* sqlite3_step() has finished executing */
)
const (
	backup_raise_err_on_busy     = "backup-raise-err-on-busy"
	backup_raise_err_on_dblocked = "backup-raise-err-on-dblocked"
)

// open flags
const (
	SQLITE_OPEN_READONLY      = 0x00000001 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_READWRITE     = 0x00000002 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_CREATE        = 0x00000004 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_DELETEONCLOSE = 0x00000008 /* VFS only */
	SQLITE_OPEN_EXCLUSIVE     = 0x00000010 /* VFS only */
	SQLITE_OPEN_AUTOPROXY     = 0x00000020 /* VFS only */
	SQLITE_OPEN_URI           = 0x00000040 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_MEMORY        = 0x00000080 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_MAIN_DB       = 0x00000100 /* VFS only */
	SQLITE_OPEN_TEMP_DB       = 0x00000200 /* VFS only */
	SQLITE_OPEN_TRANSIENT_DB  = 0x00000400 /* VFS only */
	SQLITE_OPEN_MAIN_JOURNAL  = 0x00000800 /* VFS only */
	SQLITE_OPEN_TEMP_JOURNAL  = 0x00001000 /* VFS only */
	SQLITE_OPEN_SUBJOURNAL    = 0x00002000 /* VFS only */
	SQLITE_OPEN_SUPER_JOURNAL = 0x00004000 /* VFS only */
	SQLITE_OPEN_NOMUTEX       = 0x00008000 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_FULLMUTEX     = 0x00010000 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_SHAREDCACHE   = 0x00020000 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_PRIVATECACHE  = 0x00040000 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_WAL           = 0x00080000 /* VFS only */
	SQLITE_OPEN_NOFOLLOW      = 0x01000000 /* Ok for sqlite3_open_v2() */
	SQLITE_OPEN_EXRESCODE     = 0x02000000 /* Extended result codes */
)
