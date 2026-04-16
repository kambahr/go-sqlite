// Copyright (C) 2024 Kamiar Bahri.
// Use of this source code is governed by
// Boost Software License - Version 1.0
// that can be found in the LICENSE file.

package gosqlite

// #include <stdio.h>
// #include <stdlib.h>
// #include "sqlite3.h"
import "C"
import (
	"strings"
	"time"
)

type Post struct {
	Queue []PostItem
	// IPost *IPost
}

type PostItem struct {
	Query         string
	PlaceHolders  []any
	DBFilePath    string
	NotAfterTime  time.Time
	ExpectedError string
	// TODO: calback url?
	// TODO: more options
}

type IPost interface {
	// ExecWithRetry executes a query immedialtely; if failed,
	// it puts the query in a queue to be attempted later. notAfterTime:
	// no more attempts beyond this time. expectedErrPhrase: if this error
	// is encountered, query will be attempted agains in 250 millisecond
	// intervals, otherwise the error will be retunred and no more attempts
	// will be made.
	ExecWithRetry(query string, placeHolders []any, dbFilePath string,
		expectedErrPhrase string, notAfterTime time.Time) error

	addToQueue(pi PostItem)
	processQueue()

	// exec opesn the database, executes the query, closes the database.
	exec(pi PostItem) error
}

func postInit() IPost {
	var pItems []PostItem
	var ip IPost = &Post{Queue: pItems}

	// go ip.processQueue()

	return ip
}

func (p *Post) removeFromQueue(i int) {
	p.Queue[len(p.Queue)-1], p.Queue[i] = p.Queue[i], p.Queue[len(p.Queue)-1]
	p.Queue = p.Queue[:len(p.Queue)-1]
}

func (p *Post) exec(pi PostItem) error {
	dx, err := Open(pi.DBFilePath, "PRAGMA main.secure_delete = ON")
	if err != nil {
		dx.Close()
		return err
	}

	defer dx.Close()

	_, err = dx.ExecuteNonQuery(pi.Query, pi.PlaceHolders...)

	return err
}

func (p *Post) processQueue() {
lblAgain:
	for i := range p.Queue {
		pi := p.Queue[i]
		d := time.Since(pi.NotAfterTime)
		elapsedSeonds := (d.Hours() / 60 / 60) + (d.Minutes() / 60) + d.Seconds()
		if elapsedSeonds > 0.0 {
			p.removeFromQueue(i)
			goto lblAgain
		}

		err := p.exec(pi)
		if err == nil {
			// succeeded; remove from queue
			p.removeFromQueue(i)
			goto lblAgain
		}
	}
	if len(p.Queue) > 0 {
		C.sqlite3_sleep(250)
		goto lblAgain
	}
}

func (p *Post) addToQueue(pi PostItem) {
	idxMatched := -1
	for i := range p.Queue {
		if p.Queue[i].DBFilePath == pi.DBFilePath &&
			p.Queue[i].ExpectedError == pi.ExpectedError &&
			p.Queue[i].NotAfterTime.Equal(pi.NotAfterTime) &&
			p.Queue[i].Query == pi.Query {
			idxMatched = i
			break
		}
	}

	okToAdd := true
	if idxMatched > -1 {
		for i := range pi.PlaceHolders {
			if p.Queue[idxMatched].PlaceHolders[i] != pi.PlaceHolders[i] {
				okToAdd = false
				break
			}
		}
	}

	if okToAdd {
		p.Queue = append(p.Queue, pi)
	}
}

func (p *Post) ExecWithRetry(query string, placeHolders []any, dbFilePath string,
	expectedErrPhrase string, notAfterTime time.Time) error {
	var err error
	db := GetOpenedDB(dbFilePath)
	if db == nil {
		db, err = Open(dbFilePath, "PRAGMA main.secure_delete = ON")
		if err != nil {
			db.Close()
			return err
		}
		defer db.Close()
	}

	if placeHolders == nil {
		placeHolders = make([]any, 0)
	}

	go func() {
		_, err = db.ExecuteNonQuery(query, placeHolders...)
		if err != nil {
			if strings.Contains(err.Error(), expectedErrPhrase) {
				p.addToQueue(PostItem{
					PlaceHolders:  placeHolders,
					DBFilePath:    dbFilePath,
					Query:         query,
					NotAfterTime:  notAfterTime,
					ExpectedError: expectedErrPhrase,
				})

				go p.processQueue()

				return
			}
		}
	}()

	return err
}
