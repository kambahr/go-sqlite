// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

package gosqlite

//#include "sqlite3.h"
import "C"

// go_xProgress is the pregress on the number of content pages
// written to the source database. See BackupOnlineDBFile().
//
//export go_xProgress
func go_xProgress(x C.int, y C.int, pDb *C.sqlite3, err *C.char, userData *C.char) {

	// Find the caller's database in the group; and call the
	// progress callback if it is set.
	for i := 0; i < len(DBGrp.Base().OpenDatabases); i++ {
		if DBGrp.Base().OpenDatabases[i].DBHwnd == pDb {
			// call the callback func if present.
			if DBGrp.Base().OpenDatabases[i].BackupProgress != nil {
				DBGrp.Base().OpenDatabases[i].BackupProgress(int(x), int(y), C.GoString(err), C.GoString(userData))
			}
			break
		}
	}
}
