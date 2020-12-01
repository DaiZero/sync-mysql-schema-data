package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
)

// DbIndex 数据库索引
type DbIndex struct {
	IndexType      indexType `json:"index_type"`
	Name           string    `json:"name"`
	SQL            string    `json:"sql"`
	RelationTables []string  `json:"relation_tables"` //相关联的表
}

type indexType string

const (
	// 主键索引
	indexTypePrimary indexType = "PRIMARY"
	// 正常索引
	indexTypeIndex = "INDEX"
	// 外键索引
	indexTypeForeignKey = "FOREIGN KEY"
)

//alterAddSQL 输出新增索引的SQL语句
func (idx *DbIndex) alterAddSQL(drop bool) string {
	var alterSQL []string
	if drop {
		dropSQL := idx.alterDropSQL()
		if dropSQL != "" {
			alterSQL = append(alterSQL, dropSQL)
		}
	}

	switch idx.IndexType {
	case indexTypePrimary:
		alterSQL = append(alterSQL, "ADD "+idx.SQL)
	case indexTypeIndex, indexTypeForeignKey:
		alterSQL = append(alterSQL, fmt.Sprintf("ADD %s", idx.SQL))
	default:
		log.Fatalln("unknown indexType", idx.IndexType)
	}
	return strings.Join(alterSQL, ",\n")
}

// String 格式化数据库索引
func (idx *DbIndex) String() string {
	bs, _ := json.MarshalIndent(idx, "  ", " ")
	return string(bs)
}

//alterDropSQL 输出索引删除语句
func (idx *DbIndex) alterDropSQL() string {
	switch idx.IndexType {
	case indexTypePrimary:
		return "DROP PRIMARY KEY"
	case indexTypeIndex:
		return fmt.Sprintf("DROP INDEX `%s`", idx.Name)
	case indexTypeForeignKey:
		return fmt.Sprintf("DROP FOREIGN KEY `%s`", idx.Name)
	default:
		log.Fatalln("unknown indexType", idx.IndexType)
	}
	return ""
}

//addRelationTable 新增关联表
func (idx *DbIndex) addRelationTable(table string) {
	table = strings.TrimSpace(table)
	if table != "" {
		idx.RelationTables = append(idx.RelationTables, table)
	}
}

//匹配索引字段
var indexReg = regexp.MustCompile(`^([A-Z]+\s)?KEY\s`)

//匹配外键
var foreignKeyReg = regexp.MustCompile("^CONSTRAINT `(.+)` FOREIGN KEY.+ REFERENCES `(.+)` ")

//parseDbIndexLine 解析索引行
func parseDbIndexLine(line string) *DbIndex {
	line = strings.TrimSpace(line)
	idx := &DbIndex{
		SQL:            line,
		RelationTables: []string{},
	}
	if strings.HasPrefix(line, "PRIMARY") {
		idx.IndexType = indexTypePrimary
		idx.Name = "PRIMARY KEY"
		return idx
	}

	//  UNIQUE KEY `idx_a` (`a`) USING HASH COMMENT '注释',
	//  FULLTEXT KEY `c` (`c`)
	//  PRIMARY KEY (`d`)
	//  KEY `idx_e` (`e`),
	if indexReg.MatchString(line) {
		arr := strings.Split(line, "`")
		idx.IndexType = indexTypeIndex
		idx.Name = arr[1]
		return idx
	}

	//CONSTRAINT `busi_table_ibfk_1` FOREIGN KEY (`repo_id`) REFERENCES `repo_table` (`repo_id`)
	foreignMatches := foreignKeyReg.FindStringSubmatch(line)
	if len(foreignMatches) > 0 {
		idx.IndexType = indexTypeForeignKey
		idx.Name = foreignMatches[1]
		idx.addRelationTable(foreignMatches[2])
		return idx
	}

	log.Fatalln("db_index parse failed,unsupported,line:", line)
	return nil
}
