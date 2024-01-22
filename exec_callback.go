// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

import "C"
import (
	"strconv"
	"unsafe"
)

// go_sqlite3_exec_callback is a callback func for Execute().
//
//export go_sqlite3_exec_callback
func go_sqlite3_exec_callback(NotUsed *C.void, argc C.int, argv **C.char, azColName **C.char, queryID *C.char) {

	qryID := C.GoString(queryID)
	size := int(argc)
	vals := (*[1 << 30]*C.char)(unsafe.Pointer(argv))[:size:size]
	cols := (*[1 << 30]*C.char)(unsafe.Pointer(azColName))[:size:size]

	mRow := make(map[string]any, 1)

	for i := 0; i < size; i++ {

		// the value-type of the argv is string;
		// set the value accoring to its type.

		var v any

		s := C.GoString(vals[i])

		valInt, err := strconv.Atoi(s)
		if err == nil {
			v = valInt

		} else {
			v = s
		}

		mRow[C.GoString(cols[i])] = v
	}

	l := len(mResultQueue)
	for i := 0; i < l; i++ {
		if mResultQueue[i].QueryID == qryID {
			mResultQueue[i].Result = append(mResultQueue[i].Result, mRow)
			return
		}
	}
}
