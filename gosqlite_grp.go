// The author disclaims copyright to this source code
// as it is dedicated to the public domain.
// For more information, please refer to <https://unlicense.org>.

package gosqlite

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

func (g *DBGroup) bgProc() {
	// Ping the databases. Remove the ones that
	// do not respond.
	for {
		for i := 0; i < len(g.OpenDatabases); i++ {

			err := g.Ping(g.OpenDatabases[i])
			msgTxt := "ok"
			if err != nil {
				msgTxt = err.Error()
			}

			if g.Verbose {
				log.Println("ping:", g.OpenDatabases[i].Name, msgTxt)
			}

			if err != nil {
				// remove from the list
				newArr := make([]*DB, 0)
				for j := 0; j < len(g.OpenDatabases); j++ {
					if g.OpenDatabases[j].UniqueName != g.OpenDatabases[i].UniqueName {
						newArr = append(newArr, g.OpenDatabases[j])
					}
				}
				g.OpenDatabases = newArr

				break

			} else {

				db, err := g.Get(g.OpenDatabases[i].FilePath())
				if err != nil {
					if g.Verbose {
						log.Println(g.OpenDatabases[i].Name, "=>", err)
					}
				} else {

					// optimize
					_, err := db[0].Execute("PRAGMA optimize;")
					if err != nil {
						if g.Verbose {
							log.Println(g.OpenDatabases[i].Name, "=>", err)
						}
					}
					// vacuum the db
					m, err := db[0].ExecuteScalare("PRAGMA freelist_count;")
					if err != nil {
						if g.Verbose {
							log.Println(g.OpenDatabases[i].Name, "=>", err)
						}
					} else {
						if m != nil && m.(int64) > 1500 && !g.OpenDatabases[i].Busy() {
							if g.Verbose {
								log.Println("<<< VACUUM >>>", g.OpenDatabases[i].Name)
							}
							err = g.OpenDatabases[i].Vacuum()
							if err != nil && g.Verbose {
								log.Println(err)
							} else {
								g.OpenDatabases[i].ShrinkMemory()
							}
						}
					}
				}
			}

			time.Sleep(time.Second)
		}

		time.Sleep(193 * time.Second)
	}
}

// Base exposes all properties of DBGroup
// to the caller (methods and variables).
func (m *DBGroup) Base() *DBGroup {
	return m
}

func initGrouper() {

	if len(DBGrp.Base().OpenDatabases) > 0 {
		return
	}

	DBGrp = &DBGroup{}

	go DBGrp.bgProc()
}

func (m *DBGroup) Count() int {
	return len(m.OpenDatabases)
}

func (m *DBGroup) Get(srchTxt string) ([]*DB, error) {

	var arryDB []*DB

	sLower := strings.ToLower(srchTxt)

	for i := 0; i < m.Count(); i++ {
		if m.OpenDatabases[i].UniqueName == srchTxt {
			arryDB = append(arryDB, m.OpenDatabases[i])

		} else if strings.ToLower(m.OpenDatabases[i].FilePath()) == sLower {
			arryDB = append(arryDB, m.OpenDatabases[i])

		} else if strings.Contains(strings.ToLower(m.OpenDatabases[i].FilePath()), sLower) {
			arryDB = append(arryDB, m.OpenDatabases[i])
		}
	}

	if len(arryDB) == 0 {

		// try to open the database, if the file exists on disk.
		fp := srchTxt
		if fileOrDirExists(srchTxt) {
			// db, err := Open(fp)
			db, err := Open(fp,
				/*"PRAGMA main.journal_mode = DELETE",*/
				"PRAGMA main.secure_delete = ON")

			if err != nil {
				/* empty array */
				arryDB = append(arryDB, &DB{})
			} else {
				/* opened successfully */
				//db.JournalMode = "DELETE"
				arryDB = append(arryDB, db)
				m.Add(db)
				return arryDB, err
			}
		} else {
			/* return an empty db, so that the caller does not
			get null-pointer panic */
			arryDB = append(arryDB, &DB{})
			return arryDB, errors.New("no such database in the DBGroup")
		}
	}

	/* db already existed in the grp*/
	return arryDB, nil
}

func (m *DBGroup) Ping(db *DB) error {

	if db.Ping() == 0 {
		return nil
	}
	return errors.New("bad connection")
}

func (g *DBGroup) Add(db *DB) {
	if g == nil || db == nil {
		return
	}
	for i := 0; i < len(g.OpenDatabases); i++ {
		if g.OpenDatabases[i].UniqueName == db.UniqueName {
			return
		}
	}
	g.OpenDatabases = append(g.OpenDatabases, db)
	//go db.daemon()
}

// Remove closes the database and removes it from
// the array of OpenDatabase.
func (g *DBGroup) Remove(db *DB) {
	if db == nil {
		return
	}
	indx := -1
	for i := 0; i < len(g.OpenDatabases); i++ {
		if g.OpenDatabases[i].UniqueName == db.UniqueName {
			indx = i
			break
		}
	}
	if indx < 0 {
		return
	}
	err := db.Close()
	if err != nil {
		// bad parameter or other API misuse: means the database is already closed.
		if !strings.Contains(err.Error(), "bad parameter or other API misuse") &&
			!strings.Contains(err.Error(), "not an error") {
			fmt.Println("DBGroups.Remove() =>", err)
		}
	}
	db = nil

	// remove from array of OpenDatabases
	g.OpenDatabases[len(g.OpenDatabases)-1], g.OpenDatabases[indx] = g.OpenDatabases[indx], g.OpenDatabases[len(g.OpenDatabases)-1]
	g.OpenDatabases = g.OpenDatabases[:len(g.OpenDatabases)-1]
}
