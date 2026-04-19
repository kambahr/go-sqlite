# go-sqlite

A highly performant, thread-safe, and feature-rich SQLite wrapper for Go, relying directly on CGO and `sqlite3.c` internally. 

`go-sqlite` is designed for applications needing robust concurrent access to an SQLite database without worrying about C pointer desyncs or manual locking. It offers powerful object-oriented data retrieval (`DataTable`), automatic schema querying, and background maintenance.

## ✨ Key Features & Benefits

* **Battle-Tested Concurrency:** Built-in mutexes and queueing (`DBGroup`, internal execution queues) completely safeguard multithreaded operations. The driver gracefully retries on `database is locked` events, sparing developers from manual retry loops.
* **The `DataTable` Abstraction:** Why iterate pointers manually when you don't have to? Fetch complete result sets instantly into an iterable, JSON/CSV-exportable `DataTable`.
* **Automatic Background Maintenance:** Through the `DBGroup` manager, your databases are periodically pinged to ensure connection health and are automatically `VACUUM`ed in the background when the freelist page counts exceed performance thresholds. 
* **Dynamic Type Scanning:** `Rows.Scan()` dynamically detects underlying SQLite runtime types (TEXT, INTEGER, BLOB, REAL) and correctly maps them to your Go `int`, `string`, `bool`, or `time.Time` pointers effortlessly.
* **In-Memory & Temp Data Support:** Easily create ephemeral in-memory databases or seamlessly provision temporary tables inside physical databases.
* **Security & PRAGMA Enforcement:** Built-in hooks for encrypting/decrypting the `.sqlite` file on disk, coupled with automatic injection of high-security pragmas (like `PRAGMA main.secure_delete = ON`) upon connection initialization.

## 🚀 Quick Start

```go
package main

import (
    "fmt"
    "log"
    "[github.com/kambahr/go-sqlite](https://github.com/kambahr/go-sqlite)"
)

func main() {
    // 1. Open or Create a Database 
    // (Automatically runs PRAGMA secure_delete and tracking)
    db, err := gosqlite.Open("mydata.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close() // Removes the DB from the background tracker

    // 2. Execute Non-Query operations with automatic Retry Logic
    _, err = db.ExecuteNonQuery(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            name TEXT, 
            active INTEGER
        )
    `)

    // 3. Insert with dynamic interface{} placeholders
    res := db.Exec("INSERT INTO users (name, active) VALUES (?, ?)", "Alice", true)
    if res.Error() != nil {
        log.Fatal(res.Error())
    }

    // 4. Fetch the entire result-set using a DataTable
    dt, err := db.GetDataTable("SELECT * FROM users")
    if err == nil {
        // Easily export to JSON
        jsonString := db.DataTableToJSON(*dt)
        fmt.Println(jsonString)
        
        // Or export directly to a CSV file!
        dt.ExportToCSV("export.csv")
    }
}
```
## 🧠 Advanced Capabilities
### The DBGroup Manager
Whenever a database is opened via Open(), it is attached to the global DBGroup tracker. This daemon routine:

Pings connections safely to drop orphaned handles.

Auto-triggers PRAGMA optimize.

Monitors the freelist_count and executes a background VACUUM when empty pages pass the threshold.

### Retry Queues & The IPost interface
For mission-critical background jobs, the IPost implementation allows you to queue execution jobs via ExecWithRetry(). If a job hits a locked database, it queues itself for automated retry attempts until a given NotAfterTime limit passes.

#### Fast Conversions
The library exposes native conversion methods such as:

* Rows.GetString("column_name")

* Rows.GetInt("column_name")

* DataTable.Update() - Updates the database schema rows accurately referencing the DataRow memory.

## License
This package is governed by the Boost Software License - Version 1.0. Please refer to the LICENSE file for more details.