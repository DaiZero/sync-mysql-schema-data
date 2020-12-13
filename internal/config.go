package internal

import (
	"encoding/json"
	"log"
	"os"
)

// 配置结构体
type Config struct {
	// 源数据库地址
	SourceDSN string `json:"source"`
	// 目标数据库地址
	DestDSN string `json:"dest"`
	// 表格忽略信息集合
	AlterIgnore map[string]*AlterIgnoreTable `json:"alter_ignore"`
	// 要同步的表名集合
	Tables []string `json:"tables"`
	// 忽略的表名集合
	TablesIGNORE []string `json:"tables_ignore"`
	// 电子邮件配置
	Email *EmailConfig `json:"email"`
	// 配置地址
	ConfigPath string `json:"config_path"`
	// 是否同步表结构
	Sync bool `json:"sync"`
	// 是否使用 Drop
	Drop bool `json:"drop"`
	// 是否同步表数据
	SyncData bool `json:"sync_data"`
	// 要同步数据的表名集合
	SyncDataTables []string `json:"sync_data_tables"`
	// 同步数据是否使用Truncate
	SyncDataTruncate bool `json:"sync_data_truncate"`
}

// AlterIgnoreTable 表格忽略信息结构体
type AlterIgnoreTable struct {
	Column     []string `json:"column"`
	Index      []string `json:"index"`
	ForeignKey []string `json:"foreign"` //外键
}

// String 序列化
func (cfg *Config) String() string {
	ds, _ := json.MarshalIndent(cfg, "  ", "  ")
	return string(ds)
}

// IsIgnoreField 判断表的字段名是否忽略同步
func (cfg *Config) IsIgnoreField(table string, name string) bool {
	for tName, dit := range cfg.AlterIgnore {
		if simpleMatch(tName, table, "IsIgnoreField_table") {
			for _, col := range dit.Column {
				if simpleMatch(col, name, "IsIgnoreField_column") {
					return true
				}
			}
		}
	}
	return false
}

// CheckMatchTables 检查同步结构的表是否匹配
func (cfg *Config) CheckMatchTables(name string) bool {
	if len(cfg.Tables) == 0 {
		return true
	}
	for _, tableName := range cfg.Tables {
		if simpleMatch(tableName, name, "CheckMatchTables") {
			return true
		}
	}
	return false
}

// CheckMatchSyncTables 检查同步数据的表是否匹配
func (cfg *Config) CheckMatchSyncTables(name string) bool {
	if len(cfg.SyncDataTables) == 0 {
		return false
	}
	for _, tableName := range cfg.SyncDataTables {
		if simpleMatch(tableName, name, "CheckMatchSyncTables") {
			return true
		}
	}
	return false
}

// CheckMatchIgnoreTables 检查忽略同步结构的表是否匹配
func (cfg *Config) CheckMatchIgnoreTables(name string) bool {
	if len(cfg.TablesIGNORE) == 0 {
		return false
	}
	for _, tableName := range cfg.TablesIGNORE {
		if simpleMatch(tableName, name, "CheckMatchIgnoreTables") {
			return true
		}
	}
	return false
}

// Check 配置检测
func (cfg *Config) Check() {
	if cfg.SourceDSN == "" {
		log.Fatal("source dns is empty")
	}
	if cfg.DestDSN == "" {
		log.Fatal("dest dns is empty")
	}
	//	log.Println("config:\n", cfg)
}

// IsIgnoreIndex 检测是否为忽略的表格索引
func (cfg *Config) IsIgnoreIndex(table string, name string) bool {
	for tName, dit := range cfg.AlterIgnore {
		if simpleMatch(tName, table, "IsIgnoreIndex_table") {
			for _, index := range dit.Index {
				if simpleMatch(index, name) {
					return true
				}
			}
		}
	}
	return false
}

// IsIgnoreForeignKey 检查外键是否忽略掉
func (cfg *Config) IsIgnoreForeignKey(table string, name string) bool {
	for tName, dit := range cfg.AlterIgnore {
		if simpleMatch(tName, table, "IsIgnoreForeignKey_table") {
			for _, foreignName := range dit.ForeignKey {
				if simpleMatch(foreignName, name) {
					return true
				}
			}
		}
	}
	return false
}

// SendMailFail 发送失败的邮件
func (cfg *Config) SendMailFail(errStr string) {
	if cfg.Email == nil {
		log.Println("email conf is empty,skip send mail")
		return
	}
	_host, _ := os.Hostname()
	title := "[mysql-schema-sync][" + _host + "]failed"
	body := "error:<font color=red>" + errStr + "</font><br/>"
	body += "host:" + _host + "<br/>"
	body += "config-file:" + cfg.ConfigPath + "<br/>"
	body += "dest_dsn:" + cfg.DestDSN + "<br/>"
	pwd, _ := os.Getwd()
	body += "pwd:" + pwd + "<br/>"
	cfg.Email.SendMail(title, body)
}

// LoadConfig 加载读取配置信息
func LoadConfig(confPath string) *Config {
	var cfg *Config
	err := loadJSONFile(confPath, &cfg)
	if err != nil {
		log.Fatalln("load json conf:", confPath, "failed:", err)
	}
	cfg.ConfigPath = confPath
	return cfg
}
