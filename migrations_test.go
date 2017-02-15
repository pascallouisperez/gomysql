package mysql

import (
	"database/sql"

	. "gopkg.in/check.v1"
)

func (_ *MysqlSuite) TestMigrate_all(c *C) {
	capture := &capture{}
	err := migrate(capture, "sample")
	c.Assert(err, IsNil)
	c.Assert(capture.statements, DeepEquals, [][]interface{}{
		{createMig},
		{selectMig, "v01"},
		{"first"},
		{"second"},
		{"third"},
		{insertMig, "v01", "061da4d6749795e3afdfb437664b1fa9"},
		{selectMig, "v03"},
		{"fourth"},
		{insertMig, "v03", "c0759f2416498708841e7975566360ce"},
	})
}

func (_ *MysqlSuite) TestMigrate_skip(c *C) {
	capture := &capture{hashes: map[string]string{
		"v01": "061da4d6749795e3afdfb437664b1fa9",
	}}
	err := migrate(capture, "sample")
	c.Assert(err, IsNil)
	c.Assert(capture.statements, DeepEquals, [][]interface{}{
		{createMig},
		{selectMig, "v01"},
		{selectMig, "v03"},
		{"fourth"},
		{insertMig, "v03", "c0759f2416498708841e7975566360ce"},
	})
}

func (_ *MysqlSuite) TestMigrate_badHash(c *C) {
	capture := &capture{hashes: map[string]string{
		"v01": "bad",
	}}
	err := migrate(capture, "sample")
	c.Assert(err, NotNil)
}

type capture struct {
	hashes     map[string]string
	statements [][]interface{}
}

func (c *capture) Exec(query string, args ...interface{}) (sql.Result, error) {
	statement := []interface{}{query}
	statement = append(statement, args...)
	c.statements = append(c.statements, statement)
	return nil, nil
}

func (c *capture) SelectHash(query string, name string, appliedHash *string) error {
	statement := []interface{}{query, name}
	c.statements = append(c.statements, statement)

	if hash, ok := c.hashes[name]; ok {
		*appliedHash = hash
		return nil
	}
	return sql.ErrNoRows
}
