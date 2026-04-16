package gosqlite

// #include <stdio.h>
// #include <stdlib.h>
// #include "sqlite3.h"
import "C"

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
)

func Sleep(millSec int) {
	C.sqlite3_sleep(C.int(millSec))
}

func normalizeSQL(src string) string {
	// 1️⃣ Strip comments (choose any of the three implementations above)
	cleaned := stripSQLComments(src) // or stripCommentsLex, etc.

	// 2️⃣ Collapse all runs of whitespace (spaces, tabs, newlines) into a single space.
	ws := regexp.MustCompile(`\s+`)
	normalized := ws.ReplaceAllString(strings.TrimSpace(cleaned), " ")
	return normalized
}

// stripSQLComments scans line‑by‑line, tracks whether we are inside a /* … */ block.
func stripSQLComments(src string) string {
	var out bytes.Buffer
	inBlock := false

	scanner := bufio.NewScanner(strings.NewReader(src))
	for scanner.Scan() {
		line := scanner.Text()

		// Handle block‑comment start/end on the same line first.
		if !inBlock {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				// Anything before the start is kept.
				out.WriteString(line[:idx])
				// Look for the closing delimiter on the same line.
				if endIdx := strings.Index(line[idx+2:], "*/"); endIdx >= 0 {
					// Closing on the same line – keep the rest after it.
					out.WriteString(line[idx+2+endIdx+2:])
				} else {
					// Opening without a closing – we are now inside a block.
					inBlock = true
				}
				out.WriteByte('\n')
				continue
			}
		} else {
			// We are currently inside a block comment.
			if endIdx := strings.Index(line, "*/"); endIdx >= 0 {
				// End of block comment found – keep the remainder of the line.
				out.WriteString(line[endIdx+2:])
				inBlock = false
				out.WriteByte('\n')
				continue
			}
			// Entire line is inside a block comment → skip it.
			continue
		}

		// At this point we are not inside a block comment.
		// Strip line comment (--) if present.
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		out.WriteString(strings.TrimRight(line, " \t"))
		out.WriteByte('\n')
	}
	return out.String()
}

// // removeSQLComments strips /* … */ block comments and -- line comments.
// // It does **not** try to detect comments that live inside quoted strings.
// func removeSQLComments(src string) string {
// 	// 1️⃣ Remove block comments (/* … */) – non‑greedy, dot matches newline.
// 	blockRe := regexp.MustCompile(`(?s)/\*.*?\*/`)
// 	src = blockRe.ReplaceAllString(src, "")

// 	// 2️⃣ Remove line comments (--) – stop at newline or end‑of‑input.
// 	lineRe := regexp.MustCompile(`(?m)--[^\n\r]*`)
// 	src = lineRe.ReplaceAllString(src, "")

// 	// 3️⃣ Trim excess whitespace (optional)
// 	// Collapse multiple blank lines into a single newline.
// 	src = strings.TrimSpace(src)
// 	src = regexp.MustCompile(`\n{2,}`).ReplaceAllString(src, "\n")

// 	return src
// }

// func removeSQLComments(sqlx string) (retSQL string) {

// 	retSQL = sqlx

// 	hasComments := (strings.Contains(retSQL, "/*") && strings.Contains(retSQL, "*/"))
// 	mayHaveInlineComments := strings.Contains(retSQL, "--")

// 	if hasComments {
// 		retSQL = replacePhraseInString(retSQL, "/*", "*/")
// 	}

// 	if mayHaveInlineComments {
// 		v := strings.Split(retSQL, "\n")
// 		for i := range v {
// 			v[i] = strings.Split(v[i], "--")[0]
// 		}

// 		retSQL = strings.Join(v, "")
// 	}

// 	///////////
// 	if hasComments {
// 		fmt.Println(retSQL)
// 	}
// 	///////////

// 	return
// }

// func replacePhraseInString(txt string, left string, right string) (retTxt string) {
// 	var s []string
// 	r := regexp.MustCompile(`(?s)` + regexp.QuoteMeta(left) + `(.*?)` + regexp.QuoteMeta(right))
// 	matches := r.FindAllStringSubmatch(txt, -1)
// 	for _, v := range matches {
// 		cmnt := v[1]
// 		s = append(s, cmnt)
// 	}

// 	retTxt = txt
// 	retTxt = strings.ReplaceAll(retTxt, "\n", " ")
// 	if len(matches) > 0 {
// 		for i := range matches {
// 			orig := matches[i][0]
// 			clean := matches[i][1]
// 			retTxt = strings.ReplaceAll(retTxt, orig, clean)
// 		}
// 	}

// 	return
// }

func getRandString() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%x%x%x", b[0:4], b[4:6], b[4:8])
}

func createHash(key string) string {
	hasher := md5.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

func fileOrDirExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}

	return true
}

func removeElmFrmArry(v []interface{}, e interface{}) []interface{} {
	var r []interface{}
	count := len(v)
	for i := 0; i < count; i++ {
		if v[i] != e {
			r = append(r, v[i])
		}
	}
	return r
}

func roundNumber(n float64, percision uint32) float64 {
	return math.Round(n*math.Pow(10, float64(percision))) / math.Pow(10, float64(percision))
}

func removeDoubleSpace(str string) string {
	for {
		str = strings.ReplaceAll(str, "  ", " ")
		if !strings.Contains(str, "  ") {
			break
		}
	}

	return str
}

// ConvertStringToTime converts string to time. It uses a custom layout
// to get the <date><time>+<milliseconds>+<zone offset>. The following is an example:
// 2023-03-21 11:11:03.608230911 +0100 BST => time.Time
func ConvertStringToTime(dateTimeString string) (time.Time, error) {

	tEmpty, err := time.Parse(`2006-01-02T15:04:05.000000000-07:00`, `0001-01-02T15:04:05.000000000-07:00`)
	if err != nil {
		return tEmpty, err
	}

	dateTimeString = strings.TrimSpace(strings.ReplaceAll(dateTimeString, "  ", " "))

	// start with the most common layout.
	layout := `2006-01-02T15:04:05.000000000-07:00`

	// The zone descritpion (e.g. BST) is not required.
	// the offset identifies the location (i.e. +100)
	v := strings.Split(dateTimeString, "+")
	if len(v) < 2 && len(dateTimeString) < 21 {
		// add and assume it's utc
		dateTimeString = fmt.Sprintf("%s.00000000 +0000 UTC", dateTimeString)
		v = strings.Split(dateTimeString, "+")
	}
	left := strings.TrimRight(v[0], " ")
	left = strings.ReplaceAll(left, " ", "T")

	if len(v) < 2 {
		return tEmpty, errors.New("invalid format")
	}

	offset := fmt.Sprintf("%s:%s", v[1][:2], v[1][2:4])
	s := fmt.Sprintf("%s+%s", left, offset)

	// milliseconds 9 precision.
	t, err := time.Parse(layout, s)
	if err != nil {
		// milliseconds between 3 to 9 precision.
		lyLeft := `2006-01-02T15:04:05.000`
		lyRight := `-07:00`

		for i := range 6 {
			layout = fmt.Sprintf("%s%s%s", lyLeft, strings.Repeat("0", i+1), lyRight)
			t, err = time.Parse(layout, s)
			if err == nil {
				return t, nil
			}
		}

		return tEmpty, err
	}

	return t, nil
}

func EncryptLight(data []byte, passphrase string) ([]byte, error) {
	block, _ := aes.NewCipher([]byte(createHash(passphrase)))
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

func DecryptLight(data []byte, passphrase string) ([]byte, error) {
	var clearb []byte
	if len(data) == 0 {
		return nil, errors.New("data is empty; nothing to decrypt")
	}
	key := []byte(createHash(passphrase))
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if nonceSize < 1 {
		return clearb, nil
	}
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	clearb, err = gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return clearb, nil
}

func prunDoubleSpace(s string) string {
	for {
		if !strings.Contains(s, "  ") {
			break
		}
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return s
}

// RemoveCommentsFromString removes a block of text from inside an string.
func RemoveCommentsFromString(s string, begin string, end string) string {

	begin = strings.Trim(begin, " ")
	end = strings.Trim(end, " ")

lblAgain:
	i := strings.Index(s, begin)

	if i < 0 {
		// not found
		return s
	}

	left := s[:i]

	right := s[len(left):]

	j := strings.Index(right, end)
	right = right[j+len(end):]

	s = fmt.Sprintf("%s%s", left, right)

	i = strings.Index(s, begin)

	if i > -1 {
		goto lblAgain
	}

	return s
}
