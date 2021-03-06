package dbtest

import (
	"database/sql"
	"fmt"

	"github.com/pascallouisperez/gomysql"

	. "gopkg.in/check.v1"
)

// getTestDatabase gets a test database which is ready for tests to be ran
// against.
func GetTestDatabase(c *C, dbName string, migrations ...string) *sql.DB {
	createTestDatabase(c, dbName)

	dsn := gomysql.DefaultMysqlDsn()
	dsn.DbName = dbName
	db, err := dsn.Open()
	c.Assert(err, IsNil)

	for _, migrationPath := range migrations {
		err = gomysql.Migrate(db, migrationPath)
		c.Assert(err, IsNil)
	}

	return db
}

// createTestDatabase creates a fresh database, dropping a pre-existing one if
// needed.
// This function either succeeds, or aborts the test, and can therefore
// be used as a statement.
func createTestDatabase(c *C, dbName string) {
	dsn := gomysql.DefaultMysqlDsn()
	dsn.DbName = ""

	db, err := dsn.Open()
	c.Assert(err, IsNil)
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	c.Assert(err, IsNil)

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
	c.Assert(err, IsNil)
}
