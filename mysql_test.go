package gomysql

import (
	"gopkg.in/yaml.v2"

	. "gopkg.in/check.v1"
)

func (_ *MysqlSuite) TestMysqlDsn(c *C) {
	examples := map[string]MysqlDsn{
		"/": MysqlDsn{},

		"/dbname": MysqlDsn{
			DbName: "dbname",
		},

		"root@/": MysqlDsn{
			Username: "root",
		},
		"root:password@/": MysqlDsn{
			Username: "root",
			Password: "password",
		},
		"/ignore-if-no-password": MysqlDsn{
			Password: "password",
			DbName:   "ignore-if-no-password",
		},

		"tcp(localhost)/test": MysqlDsn{
			Protocol: "tcp",
			Address:  "localhost",
			DbName:   "test",
		},
		"/ignore-if-no-protocol": MysqlDsn{
			Address: "localhost",
			DbName:  "ignore-if-no-protocol",
		},

		"/?param1=value1": MysqlDsn{
			Params: map[string]string{
				"param1": "value1",
			},
		},
	}
	for formatted, dsn := range examples {
		c.Log(formatted)
		c.Assert(dsn.String(), Equals, formatted)
	}
}

type SampleConfig struct {
	Single MysqlDsn            `yaml:"single"`
	Multi  map[string]MysqlDsn `yaml:"multi"`
}

func (_ *MysqlSuite) TestMysqlDsn_defaults(c *C) {
	dsn := DefaultMysqlDsn()
	c.Assert(dsn.Username, Equals, "root")
	c.Assert(dsn.Password, Equals, "")
	c.Assert(dsn.Protocol, Equals, "tcp")
	c.Assert(dsn.Address, Equals, "localhost:3306")
	c.Assert(dsn.DbName, Equals, "")
	c.Assert(dsn.Params, DeepEquals, map[string]string{
		"strict":    "true",
		"sql_notes": "false",
	})
}

func (_ *MysqlSuite) TestMysqlDsn_fromYaml(c *C) {
	data := `
single:
  username: username_single
  password: password_single
  protocol: protocol_single
  address: address_single
  dbname: dbname_single
  params:
    param01: value01
    param02: value02
multi:
  ds01:
    username: username_ds01
`

	cfg := SampleConfig{}
	err := yaml.Unmarshal([]byte(data), &cfg)
	c.Assert(err, IsNil)

	c.Assert(cfg.Single.Username, Equals, "username_single")
	c.Assert(cfg.Single.Password, Equals, "password_single")
	c.Assert(cfg.Single.Protocol, Equals, "protocol_single")
	c.Assert(cfg.Single.Address, Equals, "address_single")
	c.Assert(cfg.Single.DbName, Equals, "dbname_single")
	c.Assert(cfg.Single.Params, DeepEquals, map[string]string{
		"param01": "value01",
		"param02": "value02",
	})

	c.Assert(cfg.Multi["ds01"].Username, Equals, "username_ds01")
}
