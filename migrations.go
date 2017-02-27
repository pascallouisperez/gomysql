package gomysql

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"github.com/golang/glog"
	"github.com/pascallouisperez/goutil/errors"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"
)

var createMig = `
CREATE TABLE IF NOT EXISTS migration (
	id bigint not null auto_increment,
	name varchar(255),
	hash varchar(255),
	primary key (id),
	unique index (name)
)
`

var selectMig = `
SELECT hash FROM migration WHERE name = ?
`

var insertMig = `
INSERT INTO migration (name, hash) VALUES (?, ?)
`

type wrap struct {
	*sql.DB
}

func (w *wrap) SelectHash(query string, name string, appliedHash *string) error {
	return w.DB.QueryRow(query, name).Scan(appliedHash)
}

// Assert wrap implements the executor interface.
var _ executor = &wrap{}

func Migrate(db *sql.DB, home string) error {
	err := migrate(&wrap{db}, home)
	if err != nil {
		return errors.New("migration: %s", err)
	}
	return nil
}

// executor defines the smallest part of the sql.DB methods we need to run
// migrations. The indirection is meant to simplify testing.
type executor interface {
	SelectHash(string, string, *string) error
	Exec(string, ...interface{}) (sql.Result, error)
}

func migrate(exec executor, home string) error {
	// Grab all migrations
	files, err := filepath.Glob(filepath.Join(home, "v*__*.sql"))
	if err != nil {
		return err
	}

	// Ensure we go through them in order
	sort.Sort(sort.StringSlice(files))

	// Migrating
	if _, err = exec.Exec(createMig); err != nil {
		return err
	}
	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}

		name := name(file)
		hash := hash(data)

		var appliedHash string
		skip := true
		err = exec.SelectHash(selectMig, name, &appliedHash)
		if err != nil {
			if err == sql.ErrNoRows {
				skip = false
			} else {
				return err
			}
		}
		if skip {
			if hash != appliedHash {
				return errors.New("name=%s has hash of %s but applied hash is %s", name, hash, appliedHash)
			}
			continue
		}

		glog.Infof("migrations: applying %s", name)
		for _, statement := range strings.Split(string(data), ";") {
			if strings.TrimSpace(statement) != "" {
				if _, err := exec.Exec(statement); err != nil {
					return err
				}
			}
		}
		if _, err = exec.Exec(insertMig, name, hash); err != nil {
			return err
		}
	}
	return nil
}

func name(file string) string {
	base := filepath.Base(file)
	split := strings.Split(base, "__")
	return split[0] // safe since files match glob pattern
}

func hash(data []byte) string {
	hash16 := md5.Sum(data)
	return hex.EncodeToString([]byte{
		hash16[0], hash16[1], hash16[2], hash16[3],
		hash16[4], hash16[5], hash16[6], hash16[7],
		hash16[8], hash16[9], hash16[10], hash16[11],
		hash16[12], hash16[13], hash16[14], hash16[15],
	})
}
