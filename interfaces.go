// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

package gosqlite

import "context"

// IDB is an instance for a single database.
type IDB interface {

	// <!--
	// AutoCommit tells if auto-commit is on.
	AutoCommit() bool
	// -->

	// <!--
	// Base exposes all properties of DB to the caller
	// (methods and variables).
	Base() *DB
	// -->

	// <!--
	// Busy lets the caller know that the database is "busy" with something
	// essential and it cannot operations such as "write" or "vacuum."
	// A database is "busy" if it is not Idle, which means that one (or more)
	// of the following operations is running: Exec(), GetDataTable(),
	// Execute(), ExececuteNonQuery(), or ExececuteScalre()
	Busy() bool
	// -->

	// <!--
	// AttachDB attaches a database to the current open database.
	// Note that the same database can attached sevarl times using differnt
	// attach names. Note that the default max number of attached databases
	// is 10, but it can be increased to 125, see https://www.sqlite.org/limits.html
	//
	// ** dbFilePathToAttach ... full local path to the database to attach to
	// ** attchName ............ must start with a character
	AttachDB(dbFilePathToAttach string, attchName string) (bool, error)
	// -->

	// <!-- CloneDB copies a database using SQLite's backup feature.
	CloneDB(destPath string) error
	// -->

	// <!--
	// Close drops all connections to the database
	// and releases its file handle.
	Close() error
	// -->

	// <!--
	// CopyDatabase copies a database file into antoher.
	// The destination file must not exist.
	CopyDatabase(dbFilePath string) error
	// -->

	// <!--
	// CopyTableToDatabase copies a table from the current database to another.
	// If append is set to true, all rows will be appended to the target table,
	// otherwise a new table is created; if the table exists it will be re-name
	// (as <table name>_n). If recreateAutoIncrementCol is set to true the
	// auto-increment column will be re-create so that the integer numbers are
	// reset form 1 to n.
	CopyTableToDatabase(tbleNameToCopy string, targetDBFilePath string, dropIfExists bool, append bool, CopyTableToDatabase bool) (string, error)

	// <!--
	// DataVersion executes PRAGNA data_version.
	// The return is the nuumber of the current version.
	// SQLite increments this nuumber after each insert/delete.
	DataVersion() uint32
	// -->

	// <!--
	// Describe returns a full description of a database.
	// if noDesc is true, only size and page count are
	// calculated; this is done for speed.
	Describe(noDesc ...bool) DBStat
	// -->

	DetachDB(attchName string) error

	// <!--
	// DropView closes the current connection first,
	// opens it with exclusive connection, and then reopens
	// the regular connection.
	DropView(viewName string) error
	// -->

	// <!--
	// DropTable closes the current connection first,
	// opens it with exclusive connection, and then reopens
	// the regular connection.
	DropTable(tableName string, vaccumAfter ...bool) error
	// -->

	// <!--
	// GetAttachedDatabases returns a list of database attached to the current database.
	GetAttachedDatabases() []AttachedDB
	// -->

	// <!--
	// Exec executes an query and returns a Result type.
	Exec(query string, placeHolders ...any) Result
	// -->

	// <!--
	// Exec executes an query using a context; it returns a Result type.
	ExecWithContext(ctx context.Context, query string, placeHolders ...any) Result
	// -->

	// <!--
	// Execute executes an sql statement and returns the error.
	// see: https://sqlite.org/cintro.html.
	// It does not process prepared statements; only executes satements
	// as they are.
	Execute(sqlx string) (int64, error)
	// -->

	// <!--
	// ExecuteScalare returns one value of type any (interface{}).
	ExecuteScalare(query string, placeHolders ...any) (any, error)
	// -->

	// <!--
	// ExecuteNonQuery executes a query and returns rowsAffected.
	ExecuteNonQuery(query string, placeHolders ...any) (int64, error)
	// -->

	// <!--
	// FilePath returns the value of filePath. It is
	// read-only as it is required for attaching datbases.
	FilePath() string
	// -->

	// <!--
	// GetTableColumns returns an array of Column for a
	// table. A Column is a full descropton of a table's field.
	GetTableColumns(tableName string) []Column
	// -->

	// <!--
	// GetDataTable returns query result in forms of rows/columns.
	// Due to locking the entire func, only one instance of
	// this func will be processed at a time. Since the
	// max-concurrent connections will always be 1, checking on concurrent
	// connections would not be necessary; i.e.
	//
	//	if d.ConnPool.MaxOpenConns > 0 && GetDataTableSeqNo >= d.ConnPool.MaxOpenConns {
	//		wrkRes.Err = fmt.Errorf("open connections exceeded the maximum of %d", d.ConnPool.MaxOpenConns)
	//		log.Fatal(wrkRes.Err)
	//		return &wrkRes, wrkRes.Err
	//	}
	//
	// *** on FIFO ***
	// Sequential calls to GetDataTable() are returned as FIFO,
	// however, if the caller creates multiple threads (i.e.
	//
	//	 go func() {
	//		   <calls to GetDataTable()>
	//	 }()
	//
	// then GetDataTable() returns the result as soons as it
	// receives it from sqlite3.
	// For example, with the following query:
	//
	// "select _rowid_ as RowID from my-table limit 1 offset 0"
	//
	// results feteched sequentially [ordered]
	// (i.e getMyTable() waits for result):
	//
	//	First
	//	Second
	//	Thrid
	//
	// results feteched non-sequentially [un-ordered]
	// (i.e go getMyTable() does not wait for result):
	//
	//	Second
	//	Thrid
	//	First
	//
	// ** Frequent calls and releasing unsafe poninters **
	// The process for the entire func is locked so that releasing
	// of [unsafe] pointers between sqlite3 and Go is not interrupted.
	// If this is not done, frequent calls could crash the process.
	// As the following example:
	//
	//	func myFunc (db *gosqlite.DB)
	//	/*risk on out-of-sync pointer on rapid calls*/
	//
	//	where as the following would be safe:
	//	func myFunc (db gosqlite.DB) // passed by-value
	GetDataTable(query string, placeHolders ...any) (*DataTable, error)
	// -->

	// <!--
	// GetPage returns a DataTable of a table; using LIMIT and OFFSET to
	// query on a spacific range, hence PageSize and Page Number. The result-set
	// can be filtered by a parital SQL statement and also have sort directions.
	GetPage(pageSize int64, pageNo int64, tableName string, filter string, sortBy string, sortOrder string) (DataTable, error)
	// -->

	// <!--
	// GetResultSet places the caller's request on a queue, and waits for it
	// to be done. If the caller calls this func via a thread, then the result
	// is sent back as soon as the query finishes. Although, call to this
	// package may come in concurrently, SQLite itself maintians the FIFO order.
	GetResultSet(sqlx string) QueryResult
	// -->

	// <!--
	// Interrupt causes any pending database operation to abort.
	// It returns true if interrupted.
	Interrupt() bool
	// -->

	// <!--
	// IsInMemory returns true if the database was created in-memeory.
	IsInMemory() bool
	// -->

	// <!--
	// IsColumnAutoIncrement returns whether a column is set as
	// auto-increment. It will return error if it does not exist.
	IsColumnAutoIncrement(colName string, tableName string) (bool, error)
	// -->

	// <!--
	// MemoryUsed the number of bytes of memory currently
	// outstanding (alloated but not freed).
	MemoryUsed() int64
	// -->

	// <!--
	// Ping sends the state of the database to the caller.
	// 0 => open, 1 => closed.
	Ping() int
	// -->

	// <!--
	// PageCount retuns the page count of the current database.
	PageCount() int64

	// -->
	// <!--
	// Prepare initializes an sql statement.
	// Aargs:
	// sqlx: SQL statement, UTF-8 encoded,
	// placeHolders list place-holders in the sql statement,
	// Return:
	// Stmt: A pointer to the prepared statement,
	// End of parsed string (unused portion of sql),
	// error: error
	Prepare(sqlx string, placeHolders []any) (Stmt, string, error)
	// -->

	// <!--
	// Query executes an sql statement and returns a pointer to
	// Rows for the caller to iterate.
	Query(query string, placeHolders ...any) (*Rows, error)
	// -->

	// <!--
	// RemoveAttachedDB removes currenlty attached database from the currnt open database.
	//
	// ** attachName string ... must start with a character
	RemoveAttachedDB(attachName string) error
	// -->

	// <!--
	// RemoveAllAttachedDBs removes all attached databases.
	RemoveAllAttachedDBs() error
	// -->

	// <!--
	// Rename Table renames a table.
	RenameTable(tableName string, newtableName string) error
	// -->

	//<!--
	// StmtReadOnly evaludates an SQL satement to see if it modifes anything in database.
	// From sqlitec: "This routine returns false if there is any possibility that the
	// statement might change the database file.  ^A false return does
	// not guarantee that the statement will change the database file.
	// ^For example, an UPDATE statement might have a WHERE clause that
	// makes it a no-op, but the sqlite3_stmt_readonly() result would still
	// be false.  ^Similarly, a CREATE TABLE IF NOT EXISTS statement is a
	// read-only no-op if the table already exists, but
	// sqlite3_stmt_readonly() still returns false for such a statement.
	//
	// If prepared statement X is an [EXPLAIN] or [EXPLAIN QUERY PLAN]
	// statement, then sqlite3_stmt_readonly(X) returns the same value as
	// if the EXPLAIN or EXPLAIN QUERY PLAN prefix were omitted."
	StmtReadOnly(sqlx string) bool
	//-->

	// <!-- TableExists returns true if a table exists.
	TableExists(tableName string) bool
	// -->

	// <!--
	// TotalChanges returns the global number of changes
	// in a database. It can be used to determine updates
	// occurred in a database.
	TotalChanges() int64
	// -->

	// <!--
	TruncateTable(tblName string) (err error)
	// -->

	// <!--
	// TurnOffAutoIncrement removes the auto-increment attribute of a primary-key
	// column defined as auto-increment.
	TurnOffAutoIncrement(tableName string) error
	// --->

	// <!--
	// TurnOnAutoIncrement will turn a column into an auto-increment (primary-key) column
	TurnOnAutoIncrement(tableName string, colName string, reoderAutoInrecValues ...bool) error
	// -->

	// <!--
	// TxBegin returns a txID for the transaction to be
	// committed or rolled back to.
	TxBegin() (string, error)
	// -->

	// <!--
	// TxRollback restores a savepoint to a txID.
	TxRollback(txID string) error
	// -->

	// <!--
	// TxCommit commits a transaction from a savepoint.
	TxCommit(txID string) error
	// -->

	// <!--
	// Vacuum shrinks a database.
	Vacuum(dbVacuumInto ...string) error
	// -->
}

type RmtResult struct {
	RowsAffected int64 `json:"rows-affected"`
	LastInsertId int64 `json:"last-insert-id"`
	Err          error `json:"error"`

	// ElapsedTime is the result of TimeDurration.String()
	// e.g. 31.316159ms
	ElapsedTime string `json:"elapsed-time"`
}

type Result struct {
	rowsAffected int64
	lastInsertId int64
	err          error
	// PageCount    int64

	intfce  IResult // this ensures IResult is implemented
	NotUsed string  // this is so that there is one exported field
}

// A Result summarizes an executed SQL command.
type IResult interface {
	// LastInsertId returns the last max-value of the built-in _rowid_.
	LastInsertId() (int64, error)

	// PageCount() int64

	// RowsAffected returns the number of rows affected by an
	// update, insert, or delete.
	RowsAffected() (int64, error)

	Error() error
}

// DBGroup keeps track of databases opened by
// the client app.
type DBGroup struct {
	Verbose       bool
	OpenDatabases []*DB
}

type IDBGroup interface {
	Add(db *DB)
	Remove(db *DB)
	Ping(db *DB) error
	Base() *DBGroup

	// GetDB pointes to open databases based
	// on their unique names, partial or complete filePath
	// or the friendly name. If partial, there has to be only one match
	// found, otherwise it will return an error.
	// The return []*DB is a list of open databases.
	// Note:
	//   ** a database can have multiple connections,
	//      so []*DB can have duplicate entries.
	Get(s string, pragma ...string) ([]*DB, error)
	Find(dbFilePath string) *DB
	Exists(dbFilePath string) bool

	// Count returns count of open databases
	Count() int

	// bgProc continuously pings all databases and removes the
	// unresponsive ones form the global OpenDatabases list.
	// It will also shrink (vacuum) databases from time-to-time.
	// ** shrinking will start if a database's free-page count is > 1500 and not "busy"
	//    Note that pragma_secure_delete is executed by default when a database
	//    is opened; this makes the VCUUM perform considerably faster.
	//    See https://www.sqlite.org/compile.html#secure_delete
	bgProc()
}
