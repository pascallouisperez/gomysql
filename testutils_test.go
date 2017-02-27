package gomysql

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MysqlSuite struct{}

var _ = Suite(&MysqlSuite{})
