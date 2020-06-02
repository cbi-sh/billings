package billings

import (
	"log"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

const name = ".billings"
const dsHosts = "127.0.0.1"
const replicas = "3"
const data = name + "." + "id"
const k = "Key"
const v = "Value"

var session = connect(dsHosts)

type Int64ToInt8 struct {
	Key   int64
	Value int8
}

const Error = -1
const NotSet = 0
const Contract = 1
const Prepaid = 2

func init() {
	// createConfigsInt64ToInt8(session, name, replicas)
	SelfTest()
}

func SelfTest() {

	if err := Set(0, Contract); err != nil {
		log.Fatal(err)
	}

	if b, err := Get(0); err != nil {

		log.Fatal(err)

	} else if b != Contract {

		log.Fatal("set get mismatch: ", Contract, b)

	} else {

		log.Println("self check passed")

	}

	if err := Set(0, NotSet); err != nil {
		log.Fatal(err)
	}
}

var sStmt, sNames = qb.Select(data).Where(qb.Eq(k)).ToCql()
var sQuery = gocqlx.Query(session.Query(sStmt), sNames)

func Get(id int64) (int8, error) {
	var entry []Int64ToInt8
	if err := gocqlx.Select(&entry, sQuery.BindMap(qb.M{k: id}).Query); err != nil {
		return Error, err
	} else if len(entry) == 0 {
		return NotSet, nil
	} else {
		return entry[0].Value, nil
	}
}

var uStmt, uNames = qb.Update(data).Set(v).Where(qb.Eq(k)).ToCql()
var uQuery = gocqlx.Query(session.Query(uStmt), uNames)

func Set(id int64, value int8) error {
	var entry []Int64ToInt8

	log.Println(uQuery)

	if err := gocqlx.Select(&entry, uQuery.BindMap(qb.M{k: id, v: value}).Query); err != nil {
		return err
	} else {
		return nil
	}
}

// ================================================================================================

func connect(hosts ...string) *gocql.Session {

	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.One

	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	return s
}

func exec(s *gocql.Session, stmt string) {
	q := s.Query(stmt).RetryPolicy(nil)
	defer q.Release()
	if err := q.Exec(); err != nil {
		log.Fatal(err)
	}
}

func createConfigsInt64ToInt8(s *gocql.Session, name, replicas string) {
	exec(s, `CREATE KEYSPACE IF NOT EXISTS `+name+
		` WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': `+replicas+`}`)
	exec(s, `CREATE TABLE IF NOT EXISTS `+name+"id"+
		` (Key BIGINT PRIMARY KEY, Value TINYINT)`)
}
