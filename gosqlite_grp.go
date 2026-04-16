// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

package gosqlite

//#include <stdio.h>
//#include <stdlib.h>
//#include "sqlite3.h"
import "C"
import (
	"errors"
	"log"
	"reflect"
	"strings"
)

func (g *DBGroup) bgProc() {
	// Ping the databases. Remove the ones that
	// do not respond.
	for {
		for i := range len(g.OpenDatabases) {
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
						var freeCnt int64
						if m != nil {
							if reflect.TypeOf(m).Kind().String() == "float64" {
								freeCnt = int64(m.(float64))
							} else {
								freeCnt = m.(int64)
							}
							// freelist is the number pages that are being reused..
							// This gets the db size in MB:
							// PRAGMA page_size;
							// PRAGMA freelist_count;
							// select (<page_size>.0 * <freelist_count>.0) / 1024.0 / 1024.0
							if freeCnt > 50 && !g.OpenDatabases[i].Busy() {
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
			}

			C.sqlite3_sleep(1000)
		}
		C.sqlite3_sleep(1000 * 120)
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

func (m *DBGroup) Find(dbFilePath string) *DB {
	for i := range m.Count() {
		if strings.EqualFold(m.OpenDatabases[i].FilePath(), dbFilePath) {
			return m.OpenDatabases[i]
		}
	}

	return nil
}

func (m *DBGroup) Exists(dbFilePath string) (exists bool) {
	for i := range m.Count() {
		if strings.EqualFold(m.OpenDatabases[i].FilePath(), dbFilePath) {
			return true
		}
	}

	return
}

func applyDefaultPragma(pragma ...string) []string {

	if len(pragma) == 0 {
		pragma = append(pragma, defaultPRAGMAJournalMode)
	}
	found := false
	for i := range pragma {
		s := removeDoubleSpace(pragma[i])
		if strings.EqualFold(s, "pragma main.secure_delete = on") {
			found = true
			break
		}
	}
	if !found {
		pragma = append(pragma, "PRAGMA main.secure_delete = ON")
	}

	// https://sqlite.org/pragma.html#pragma_stats
	// PRAGMA schema.synchronous = 0 | OFF | 1 | NORMAL | 2 | FULL | 3 | EXTRA;
	found = false
	for i := range pragma {
		s := removeDoubleSpace(pragma[i])
		if strings.EqualFold(s, "pragma main.synchronous") {
			found = true
			break
		}
	}
	if !found {
		pragma = append(pragma, "PRAGMA main.synchronous = OFF")
	}

	return pragma
}

func (m *DBGroup) Get(srchTxt string, pragma ...string) ([]*DB, error) {

	var arryDB []*DB

	sLower := strings.ToLower(srchTxt)

	for i := range m.Count() {
		if m.OpenDatabases[i].UniqueName == srchTxt {
			arryDB = append(arryDB, m.OpenDatabases[i])
			continue

		} else if strings.EqualFold(m.OpenDatabases[i].FilePath(), sLower) {
			arryDB = append(arryDB, m.OpenDatabases[i])
			continue

		} else if strings.Contains(strings.ToLower(m.OpenDatabases[i].FilePath()), sLower) {
			arryDB = append(arryDB, m.OpenDatabases[i])
		}
	}

	if len(arryDB) == 0 {
		// try to open the database, if the file exists on disk.
		fp := srchTxt
		if fileOrDirExists(srchTxt) {
			pragma = applyDefaultPragma(pragma...)
			db, err := Open(fp, pragma...)
			if err != nil {
				/* empty array */
				arryDB = append(arryDB, &DB{})
				return arryDB, err

			} else {
				arryDB = append(arryDB, db)
				m.Add(db)
				return arryDB, err
			}
		} else {
			// return an empty db, so that the caller does not
			// get null-pointer panic
			arryDB = append(arryDB, &DB{})
			return arryDB, errors.New("no such database in the DBGroup")
		}
	}

	// db already existed in the grp
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
	// compression option makes things slower, if the qitems
	// are small to begin with!
	// q, err := NewQueue(queue.Config{CompressItems: true})
	// iq, errQue := NewQueue()
	// if errQue != nil {
	// 	log.Fatal(errQue)
	// }
	// db.callQueue = iq
	// initQueue(db)
	g.OpenDatabases = append(g.OpenDatabases, db)

	// go db.daemon()
}

// Remove closes the database and removes it from
// the array of OpenDatabase.
func (g *DBGroup) Remove(db *DB) {
	if db == nil {
		return
	}

	mCMutex.Lock()
	defer mCMutex.Unlock()

	c := make(chan int)
	go func() {
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
				// UNDONE
			}
		}
		db = nil

		// remove from array of OpenDatabases
		if indx < len(g.OpenDatabases) {
			g.OpenDatabases[len(g.OpenDatabases)-1], g.OpenDatabases[indx] = g.OpenDatabases[indx], g.OpenDatabases[len(g.OpenDatabases)-1]
			g.OpenDatabases = g.OpenDatabases[:len(g.OpenDatabases)-1]
		}
	}()

	close(c)
}
