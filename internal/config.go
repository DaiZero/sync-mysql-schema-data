package internal

import (
	"encoding/json"
	"log"
	"os"
)

// 配置结构体
type Config struct {
	SourceDSN        string                       `json:"source"`
	DestDSN          string                       `json:"dest"`
	AlterIgnore      map[string]*AlterIgnoreTable `json:"alter_ignore"`
	Tables           []string                     `json:"tables"`
	TablesIGNORE     []string                     `json:"tables_ignore"`
	Email            *EmailConfig                 `json:"email"`
	ConfigPath       string                       `json:"config_path"`
	Sync             bool                         `json:"sync"`
	Drop             bool                         `json:"drop"`
	SyncData         bool                         `json:"sync_data"`
	SyncDataTables   []string                     `json:"sync_data_tables"`
	SyncDataTruncate bool                         `json:"sync_data_truncate"`
}

func (cfg *Config) String() string {
	ds, _ := json.MarshalIndent(cfg, "  ", "  ")
	return string(ds)
}

// AlterIgnoreTable table's ignore info
type AlterIgnoreTable struct {
	Column     []string `json:"column"`
	Index      []string `json:"index"`
	ForeignKey []string `json:"foreign"` //外键
}

// IsIgnoreField isIgnore
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

// CheckMatchTables check table is match
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

// CheckMatchSyncTables check sync table is match
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

// CheckMatchIgnoreTables check table_Ignore is match
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

// Check check config
func (cfg *Config) Check() {
	if cfg.SourceDSN == "" {
		log.Fatal("source dns is empty")
	}
	if cfg.DestDSN == "" {
		log.Fatal("dest dns is empty")
	}
	//	log.Println("config:\n", cfg)
}

// IsIgnoreIndex is index ignore
func (cfg *Config) IsIgnoreIndex(table string, name string) bool {
	for tname, dit := range cfg.AlterIgnore {
		if simpleMatch(tname, table, "IsIgnoreIndex_table") {
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
	for tname, dit := range cfg.AlterIgnore {
		if simpleMatch(tname, table, "IsIgnoreForeignKey_table") {
			for _, foreignName := range dit.ForeignKey {
				if simpleMatch(foreignName, name) {
					return true
				}
			}
		}
	}
	return false
}

// SendMailFail send fail mail
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

// LoadConfig load config file
func LoadConfig(confPath string) *Config {
	var cfg *Config
	err := loadJSONFile(confPath, &cfg)
	if err != nil {
		log.Fatalln("load json conf:", confPath, "failed:", err)
	}
	cfg.ConfigPath = confPath
	return cfg
}
