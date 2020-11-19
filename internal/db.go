package internal

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

// MyDb db struct
type MyDb struct {
	Db     *sql.DB
	dbType string
}

//NewMyDb 新建数据库连接
func NewMyDb(dsn string, dbType string) *MyDb {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		//如果连接有错误，就中断程序
		panic(fmt.Sprintf("connect to db [%s] failed,", dsn, err))
	}
	return &MyDb{
		Db:     db,
		dbType: dbType,
	}
}

// GetTableNames 获取数据库下的表名集合
func (mydb *MyDb) GetTableNames() []string {
	// 查询表信息
	rs, err := mydb.Query("show table status;")
	if err != nil {
		panic("show tableNames failed:" + err.Error())
	}
	defer rs.Close()
	var tableNames []string
	columns, _ := rs.Columns()
	for rs.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rs.Scan(valuePtrs...); err != nil {
			panic("show tableNames failed when scan," + err.Error())
		}
		var valObj = make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			valObj[col] = v
		}
		if valObj["Engine"] != nil {
			tableNames = append(tableNames, valObj["Name"].(string))
		}
	}
	return tableNames
}

// GetTableSchema table schema
func (mydb *MyDb) GetTableSchema(name string) (schema string) {
	// 获取表的创建信息
	rs, err := mydb.Query(fmt.Sprintf("show create table `%s`", name))
	if err != nil {
		log.Println(err)
		return
	}
	defer rs.Close()
	for rs.Next() {
		var vName string
		if err := rs.Scan(&vName, &schema); err != nil {
			panic(fmt.Sprintf("get table %s 's schema failed,%s", name, err))
		}
	}
	return
}

// Query 执行SQL查询语句（带入参）
func (mydb *MyDb) Query(query string, args ...interface{}) (*sql.Rows, error) {
	log.Println("[SQL]", "["+mydb.dbType+"]", query, args)
	return mydb.Db.Query(query, args...)
}

func (mydb *MyDb) QueryAll(sql string) []map[string]interface{} {
	rs, _ := mydb.Db.Query(sql)
	defer rs.Close()
	columns, _ := rs.Columns()
	var okData []map[string]interface{}
	for rs.Next() {
		var values = make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rs.Scan(valuePtrs...)
		var valObj = make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			valObj[col] = v
		}
		okData = append(okData, valObj)
	}
	return okData
}
