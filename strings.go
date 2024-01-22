// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

var j JMode

type JMode struct {
	Delete   string
	Truncate string
	Off      string
	Persist  string
	Memory   string
	Wal      string
}

func JounalMode() JMode {
	j.Delete = "DELETE"
	j.Truncate = "TRUNCATE"
	j.Off = "OFF"
	j.Memory = "MEMORY"
	j.Persist = "PERSIST"
	j.Wal = "WAL"

	return j
}

var sqlDataType DSQLiteDataType

type DSQLiteDataType struct {
	NULL    string
	TEXT    string
	INTEGER string
	REAL    string
	BLOB    string
	VARIANT string
}

func SQLiteDataType() DSQLiteDataType {
	sqlDataType.NULL = "NULL"
	sqlDataType.TEXT = "TEXT"
	sqlDataType.INTEGER = "INTEGER"
	sqlDataType.REAL = "REAL"
	sqlDataType.BLOB = "BLOB"
	sqlDataType.VARIANT = "VARIANT"

	return sqlDataType
}

var cmd CCmdParam

type CCmdParam struct {
	CollationList      string
	CheckPointFullSync string
	DataVersion        string
	ListDatabase       string
	PageCount          string
	Encoding           string
	ModuleList         string
	TableList          string
	PageSize           string
	SchemaVersion      string
}

func CmdParam() CCmdParam {
	cmd.CollationList = db_cmd_get_collation_list
	cmd.CheckPointFullSync = db_cmd_get_checkpoint_full_sync
	cmd.DataVersion = db_cmd_get_data_version
	cmd.ListDatabase = db_cmd_get_database_list
	cmd.PageCount = db_cmd_get_page_count
	cmd.Encoding = db_cmd_get_encoding
	cmd.ModuleList = db_cmd_get_module_list
	cmd.TableList = db_cmd_get_table_list
	cmd.PageSize = db_cmd_get_page_size
	cmd.SchemaVersion = db_cmd_get_schema_version

	return cmd
}

const (
	db_cmd_get_database_list        = "database_list"
	db_cmd_get_data_version         = "data_version"
	db_cmd_get_collation_list       = "collation_list"
	db_cmd_get_checkpoint_full_sync = "checkpoint_fullfsync"
	db_cmd_get_page_count           = "page_count"
	db_cmd_get_encoding             = "encoding"
	db_cmd_get_module_list          = "module_list"
	db_cmd_get_table_list           = "table_list"
	db_cmd_get_page_size            = "page_size"
	db_cmd_get_schema_version       = "schema_version"
)
