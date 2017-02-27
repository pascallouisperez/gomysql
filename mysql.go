package gomysql

import (
	"database/sql"
	"fmt"
	driver_mysql "github.com/go-sql-driver/mysql"
	"github.com/square/squalor"
	"strings"
)

// MysqlDsn provides structure to manipulate and construct a MySQL Data
// Source Name. The common format of a DSN is:
//
// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
type MysqlDsn struct {
	Username string
	Password string
	Protocol string
	Address  string
	DbName   string
	Params   map[string]string
}

func DefaultMysqlDsn() MysqlDsn {
	dsn := MysqlDsn{}
	dsn.FillDefaults()
	return dsn
}

func (dsn *MysqlDsn) FillDefaults() {
	if len(dsn.Username) == 0 {
		dsn.Username = "root"
	}
	if len(dsn.Protocol) == 0 {
		dsn.Protocol = "tcp"
	}
	if len(dsn.Address) == 0 {
		dsn.Address = "localhost:3306"
	}
	if len(dsn.Params) == 0 {
		dsn.Params = map[string]string{
			"strict":    "true",
			"sql_notes": "false",
		}
	}
}

func (dsn MysqlDsn) formattedParams() string {
	var parts []string
	for k, v := range dsn.Params {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(parts, "&")
}

func (dsn MysqlDsn) String() string {
	part1 := post(dsn.Username, pre(":", dsn.Password)+"@")
	part2 := post(dsn.Protocol, pre("(", post(dsn.Address, ")")))
	return fmt.Sprintf("%s%s/%s%s", part1, part2, dsn.DbName, pre("?", dsn.formattedParams()))
}

func (dsn MysqlDsn) Open() (*sql.DB, error) {
	return sql.Open("mysql", dsn.String())
}

func pre(prefix string, s string) string {
	if len(s) == 0 {
		return ""
	} else {
		return prefix + s
	}
}

func post(s string, postfix string) string {
	if len(s) == 0 {
		return ""
	} else {
		return s + postfix
	}
}

func IsMysqlErr(err error, number uint16) bool {
	if mysqlErr, ok := err.(*driver_mysql.MySQLError); ok {
		return mysqlErr.Number == number
	}
	return false
}

// RunTransaction runs the provided function in a database transaction,
// rolls back on error, and commits on success.
func RunTransaction(db *squalor.DB, fn func(*squalor.Tx) error) error {
	var err error
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// RunReadOnlyTransaction runs the provided function in a readd-only database
// transaction.
func RunReadOnlyTransaction(db *squalor.DB, fn func(squalor.Executor) error) error {
	if _, err := db.Exec("START TRANSACTION READ ONLY"); err != nil {
		return err
	}
	if err := fn(db); err != nil {
		db.Exec("ROLLBACK")
		return err
	}
	if _, err := db.Exec("COMMIT"); err != nil {
		return err
	}
	return nil
}
