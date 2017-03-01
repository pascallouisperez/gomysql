package dbtest

import (
	"errors"
	"testing"

	"github.com/pascallouisperez/gomysql"
	"github.com/square/squalor"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MysqlSuite struct{}

var _ = Suite(&MysqlSuite{})

func (_ *MysqlSuite) TestReadOnlyTransaction_withErr(c *C) {
	db := GetTestDatabase(c, "gomysql_testing")
	defer db.Close()

	expectedErr := errors.New("pass me around")
	actualErr := gomysql.RunReadOnlyTransaction(squalor.NewDB(db), func(exec squalor.Executor) error {
		return expectedErr
	})
	c.Assert(actualErr, Equals, expectedErr)
}

func (_ *MysqlSuite) TestReadOnlyTransaction_noErr(c *C) {
	db := GetTestDatabase(c, "gomysql_testing")
	defer db.Close()

	actualErr := gomysql.RunReadOnlyTransaction(squalor.NewDB(db), func(exec squalor.Executor) error {
		return nil
	})
	c.Assert(actualErr, IsNil)
}
