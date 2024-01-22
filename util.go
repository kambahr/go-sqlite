package gosqlite

import (
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
	"strings"
	"time"
)

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

	dateTimeString = strings.TrimSpace(dateTimeString)

	// start with the most common layout.
	layout := `2006-01-02T15:04:05.000000000-07:00`

	// The zone descritpion (e.g. BST) is not required.
	// the offset identifies the location (i.e. +100)
	v := strings.Split(dateTimeString, "+")
	left := strings.TrimRight(v[0], " ")
	left = strings.ReplaceAll(left, " ", "T")

	if len(v) < 2 {
		return tEmpty, errors.New("invalid format")
	}

	offset := v[1]
	offset = fmt.Sprintf("%s:%s", v[1][:2], v[1][2:4])
	s := fmt.Sprintf("%s+%s", left, offset)

	// milliseconds 9 precision.
	t, err := time.Parse(layout, s)
	if err != nil {
		// milliseconds between 3 to 9 precision.
		lyLeft := `2006-01-02T15:04:05.000`
		lyRight := `-07:00`

		for i := 0; i < 6; i++ {
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
