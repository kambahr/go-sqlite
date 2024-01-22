# go-sqlite
A Go driver for SQLite3. It covers all the basic + advanced functions.
<br />Full documentation: TODO.

## Feature Highlights
<ul>
<li>Reliable concurrent functionality</li>
<li>Built-in functions
    <ul>
        <li>DataTable (with built-in paging, and update(), export to CSV)</li>
        <li>Turn AutoIncrement on/off</li>
        <li>Copy tables to other databases</li>
        <li>Rename/Drop tables</li>
        <li>...</li>
    </ul>
</li>
<li>Open() database wrappers (i.e. in-memory, V2,...)</li>
<li>Automatic optimization (e.g. shrinking database files).</li>
</ul>

## Essentials

```go
func CreateDatabase(dbFilePath string) error
func Open(dbFilePath string, pragma ...string) (*DB, error)
func OpenMemory(vfsName ...string) (*DB, error)
func openV2(dbFilePath string, flag C.int, vfsName string, pragma []string) (*DB, error) 
func OpenV2Exclusive(dbFilePath string, pragma ...string) (*DB, error)
func OpenV2Readonly(dbFilePath string, pragma ...string) (*DB, error)
func OpenV2FullOption(dbFilePath string, vfsName string, flags C.int, pragma ...string) (*DB, error)
func OpenV2(dbFilePath string, pragma ...string) (*DB, error)

func (rs *Rows) Columns() ([]string, error)
func (rs *Rows) Close() error
func (r *Result) Error() error
func (d *DB) Exec(query string, placeHolders ...any) Result
func (r *Result) LastInsertId() (int64, error)
func (rs *Rows) Next() bool
func (r *Result) RowsAffected() (int64, error)
func (rs *Rows) Scan(dest ...any) error
func (d *DB) Prepare(sqlx string, placeHolders []any) (
        Stmt,   /* A pointer to the prepared statement */
        string, /* (pzTail) End of parsed string (unused portion of zSql) */
        error)
func (d *DB) TxBegin() (string, error)
func (d *DB) TxRollback(txID string) error 
func (d *DB) TxCommit(txID string) error

```

## A few things to note

### Opening Databases
You can use any of the open() functions (above) to open a database.<br />
To create a new database you must call CreateDatabase() first; as a database-file will not automatically be created
by only calling an open() function.<br />

There is no initialization via a <em>database</em> interface; i.e. open()
will send the caller's request directory to the SQLite's C code; equivalently, Close() will directly close the database file handle.<br />
Although no <em>stadard</em> interface is used, the most populare functions have been implemented
(i.e. Scan(), Next(),... see above).

### Asynchronous Operations
Asynchronous <em>read</em> operation is only implemented via GetDataTableAsync().
All other read operations are processed synchronously (FIFO).
<br>Please, note that all <em>write</em> operations are processed synchronously as SQLite3 locks the
database file for each <em>write</em> operation.

```go
GetDataTableAsync(callback func(*DataTable), query string, placeHolders ...any) (*DataTable, error) 
```

### Backup
```go
func BackupOnlineDB(
	onlineDB *DB, /* Pointer to an open database to back up */
	filePathBackupTo string, /* Full path of the file to back up to */
	millSecToSleepBeforeRepeating int, /* wait-time between copying pages */
	prgCallback func(xPagesCopied int, yTotalPages int, data string), /* Progress function to invoke */
	extraData string, /* extra data to send to callback */
	options ...string /*backup_raise_err_on_busy ir backup_raise_err_on_dblocked*/) error
```
You can take a backup of a database, while a database is online (has open file handles).
<br />You have the option of waiting between pages or take a quick backup without waiting.
<br />An additional parameter has been added to the callback func to pass extra data (e.g. JSON)
rather than only pagesCpied and totalPages.

### Execute
```go
func (d *DB) Execute(sqlx string) (int64, error)
```
Execute() uses the sqlite3_exec() function in one go rather than prepare, step,...
<br>It tends to be faster, but it does not take any prepared statements.
<br>SQLite3 offers a callback (sqlite3_exec_callback()) to get the result-set for SQL satements executed via Execute().

You can use GetResultSet() and wait for the callback to gather all result rows:
```go
func (d *DB) GetResultSet(sqlx string) QueryResult
```

### SQL tail - unused portion of an SQL statement
You can add an unrelated text to the end of your SQL satement for passing additional information.
<br />SQLite3 parses the useful part so the sql [text] and executes the SQL statement accordingly (without throwing error for the extra text).
<br />For example, use can the following SQL statement with the Exec() func:
```sql
UPDATE MyTable SET Column_1 = 'Green Dalphin' WHERE MyTableID = 99; chicago branch
```

### File Group
All databases are tracked and maintained in the background. Every time a database is opened, its file handle and description are added to a global list.
All databases in the global list are monitored and optimized (i.e. vacuumed), when they are idle.
<br />Opening databases can also be done via DBGrp.Get(<db file path>) as the following example:

```go
fp := "/my_database.sqlite"
db, _err_ := gosqlite.DBGrp.Get(fp)
if err != nil {
    log.Fatal(err)
}

t, err := db[0].InMemory.CreateTable("CREATE TEMP TABLE _Variables(Name TEXT PRIMARY KEY, RealValue REAL, IntegerValue INTEGER, BlobValue BLOB, TextValue TEXT);")
if err != nil {
    log.Fatal(err)
}

```

### Repair Corrupted Files
There many be a chance to repair a corrupted database file, if only its header is missing and/or invalid.<br>
You can use
```go
func RepairSqlite3FileStub(bDBContent []byte) []byte 
 ```
to repair a database file. RepairSqlite3FileStub() replaces the header of the database file with an
empty stub:
```go
const sqlite3_dbfile_emtpy_header = "53514c69746520666f726d6174203300100002020040202000017e1e00000009"
```


## Usage Example

```go
package main

import (
    "fmt"
    "log"
    "strings"
    "time"
    gosqlite "github.com/kambahr/go-sqlite"
)
func main() {

	// ------------- in memory -------------

	db, err := gosqlite.OpenMemory()
	if err != nil {
		log.Fatal("OpenMemory() ", strings.Repeat(".", 3), " failed ", err.Error())
	}
	defer db.Close()

	sqlx := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS my_city (
        ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
        Name TEXT NOT NULL,
        LastVisited TEXT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS INX_my_city ON my_city (Name);
        insert into my_city
        select 1,'Chicago','%s' where not exists(select 1 from my_city where Name='Chicago'); 
    `, time.Now().Add((-12820)*time.Hour))

	_, err = db.Execute(sqlx)
	if err != nil {
		log.Println("Execute() => creat table:", err)
		return
	}
	fmt.Println("create-table", strings.Repeat(".", 22), "pass")

	sqlx = fmt.Sprintf(`select * from my_city`)
	rs := db.GetResultSet(sqlx)
	if rs.Err != nil {
		log.Fatal("GetResultSet()", strings.Repeat(".", 19), " failed ", err.Error())
	}

	if len(rs.ResultTable) > 0 {
		cityName := rs.ResultTable[0]["Name"].(string)
		lstVisted := rs.ResultTable[0]["LastVisited"].(string)
		lstVistedShortDT := strings.Split(lstVisted, ".")[0]
		lstVistedShortDT = fmt.Sprintf("%s %s", lstVistedShortDT, strings.Split(lstVisted, " ")[3])

		fmt.Println("GetResultSet()", strings.Repeat(".", 20), "pass")
		fmt.Println(" City LastVisited")
		fmt.Println("", cityName, lstVistedShortDT)
	} else {
		fmt.Println("GetResultSet()", "failed result-table is empty")
		return
	}

	// ------------- with file -------------

	dbFilePath := ".../db/guitar-is-the-song.sqlite"
	_, err = os.Stat(dbFilePath)
	if os.IsNotExist(err) {
		gosqlite.CreateDatabase(dbFilePath)
	}

	sqlx = `
	CREATE TABLE IF NOT EXISTS Logs (
		LogID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
		LogType TEXT NOT NULL,
		Content BLOB NOT NULL,
		DateTimeCreated TEXT NOT NULL);`
	dbx, err := gosqlite.DBGrp.Get(dbFilePath)
	_, err = dbx[0].Execute(sqlx)
	if err != nil {
		log.Fatal(err)
	}

	var rowsAffected int64
	var res gosqlite.Result
	for i := 0; i < 100; i++ {
		sqlx = `INSERT INTO Logs(LogType,Content,DateTimeCreated)VALUES(?,?,?)`
		res = dbx[0].Exec(sqlx, "BLOB", []byte("hello world"), time.Now().String())
		rowsAffectedOne, _ := res.RowsAffected()
		rowsAffected += rowsAffectedOne
		fmt.Printf("\rinserted %d", rowsAffected)
	}
	lastID, _ := res.LastInsertId()
	fmt.Println("\nrowsAffected:", rowsAffected, "LastInsertedID:", lastID)	

	// ------------- database description -------------

	desc := dbx[0].Describe()
	fmt.Println("db size:", desc.Size, "last modified:", desc.LastModified.String()[:19])

	fmt.Printf("--- tables (%d) ---\n", len(desc.Tables))
	for i := 0; i < len(desc.Tables); i++ {
		fmt.Printf("%s%s\n", strings.Repeat(" ", 3), desc.Tables[i].Name)
		for j := 0; j < len(desc.Tables[i].Columns); j++ {
			isPrimary := ""
			if desc.Tables[i].Columns[j].IsPrimaryKey {
				isPrimary = "Primary Key"
			}
			isNull := "NULL"
			if desc.Tables[i].Columns[j].NotNULL {
				isNull = "NOT NULL"
			}
			fmt.Printf("%s%s %s %s %s\n", strings.Repeat(" ", 6),
				desc.Tables[i].Columns[j].Name, isPrimary,
				desc.Tables[i].Columns[j].DataType, isNull)
		}
	}
}

```
