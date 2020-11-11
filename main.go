package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/hidu/mysql-schema-sync/internal"
)

// 定义命令行参数对应的变量
// 配置文件地址
var configPath = flag.String("conf", "./config.json", "json config file path")

// 是否同步表结构
var sync = flag.Bool("sync", true, "sync schema change to dest db")

// 是否删除字段、索引、外键
var drop = flag.Bool("drop", false, "drop fields,index,foreign key")

// 源数据库连接字符串
var source = flag.String("source", "", "mysql dsn source,eg: test@(10.10.0.1:3306)/test\n\twhen it is not empty ignore [-conf] param")

// 目标数据库连接字符串
var dest = flag.String("dest", "", "mysql dsn dest,eg test@(127.0.0.1:3306)/user")

// 要同步的表，数组方式，支持正则
var tables = flag.String("tables", "", "table names to check,equivalent to json config [tables]\n\teg : product_base,order_*")

// 需要忽略的表，数组方式，支持正则
var tablesIGNORE = flag.String("tables_ignore", "", "table names to ignore check and ignore sync data\n\teg : product_base,order_*")

// 发送邮件
var mailTo = flag.String("mail_to", "", "overwrite config's email.to")

// 表示这个操作是同步数据，否则就是同步数据结构
var syncData = flag.Bool("sync_data", false, "sync source db table data  to dest db table")

// 表示同步源数据的时候,是否truncate本地的数据,无备份,操作需谨慎. 如果不为true,则同步数据的时候,如果目标的数据表,有自增的属性,则id的值是null,否则还是保留原有的id插入
var syncDataTruncate = flag.Bool("sync_data_truncate", false, "is need truncate source db table data  to dest db table")

// 初始化方法，先于main函数执行
func init() {
	// 定制日志的抬头信息 文件和行号，日期，时间
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
	// 输出使用方法
	df := flag.Usage
	flag.Usage = func() {
		df()
		_, _ = fmt.Fprintln(os.Stderr, "")

		// fmt.Fprintln() 输出并换行
		_, _ = fmt.Fprintln(os.Stderr, "mysql schema && data sync tools "+internal.Version)
		_, _ = fmt.Fprintln(os.Stderr, internal.AppURL+"\n")
	}
}

var cfg *internal.Config

func main() {
	//把用户传递的命令行参数解析为对应变量的值
	flag.Parse()
	// 先读取配置文件
	cfg = internal.LoadConfig(*configPath)
	// 然后命令行输入参数值覆盖配置文件里的设置值
	if *source != "" {
		cfg.SourceDSN = *source
	}
	if *dest != "" {
		cfg.DestDSN = *dest
	}
	if *sync {
		cfg.Sync = *sync
	}

	if *drop {
		cfg.Drop = *drop
	}

	if *syncData {
		cfg.SyncData = *syncData
	}
	if *syncDataTruncate {
		cfg.SyncDataTruncate = *syncDataTruncate
	}

	if *mailTo != "" && cfg.Email != nil {
		cfg.Email.To = *mailTo
	}

	if cfg.Tables == nil {
		cfg.Tables = []string{}
	}
	if cfg.TablesIGNORE == nil {
		cfg.TablesIGNORE = []string{}
	}
	if *tables != "" {
		_ts := strings.Split(*tables, ",")
		for _, _name := range _ts {
			_name = strings.TrimSpace(_name)
			if _name != "" {
				cfg.Tables = append(cfg.Tables, _name)
			}
		}
	}
	if *tablesIGNORE != "" {
		_ts := strings.Split(*tablesIGNORE, ",")
		for _, _name := range _ts {
			_name = strings.TrimSpace(_name)
			if _name != "" {
				cfg.TablesIGNORE = append(cfg.TablesIGNORE, _name)
			}
		}
	}
	defer (func() {
		if err := recover(); err != nil {
			log.Println(err)
			log.Println(fullStack())
			cfg.SendMailFail(fmt.Sprintf("%s", err))
			log.Fatalln("exit")
		}
	})()

	cfg.Check()
	if *syncData == false {
		internal.CheckSchemaDiff(cfg)
	} else {
		internal.SyncTableData(cfg)
	}

}

func fullStack() string {
	var buf [2 << 11]byte
	runtime.Stack(buf[:], true)
	return string(buf[:])
}
