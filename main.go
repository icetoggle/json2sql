package main

import (
	"fmt"
	"path"
	"path/filepath"
	"os"
	"flag"
	"io/ioutil"
	"encoding/json"
	"bytes"
	"strings"
	"strconv"
)

func main() {
	input := flag.String("in", "", "input dictionary")
	output := flag.String("out", "out.sql", "output sql")
	flag.Parse();
	var buf bytes.Buffer

	err := filepath.Walk(*input, func(filepath string, f os.FileInfo, err error) error {
		if ( f == nil ) {return err}
		if f.IsDir() {return nil}
		suf := path.Ext(filepath)
		if  (suf == ".json"){
			tableName := strings.TrimSuffix(f.Name(), suf)
			data, err := ioutil.ReadFile(filepath)
			if err != nil {
				fmt.Println("file read error:", err)
				return nil
			}
			dataJson := []byte(data)
			dataStr := []map[string]interface{}{}
			err = json.Unmarshal(dataJson, &dataStr)
			fieldMaps := make(map[string]string)
			if err != nil {
				fmt.Println("json unmarshal error", err)
			}
			if len(dataStr) == 0 {
				return nil
			}
			for _, row := range dataStr {
				for key, value := range row {
					switch value.(type) {
					case string:
						fieldMaps[key] = "string"
					case float64:
						fieldMaps[key] = "int"
					case int32:
						fieldMaps[key] = "int"
					case int64:
						fieldMaps[key] = "int"
					}
				}
			}

			for i, _ := range dataStr {
				for key, typeV := range fieldMaps {
					if dataStr[i][key] == nil {
						if typeV == "string" {
							dataStr[i][key] = ""
						} else if typeV == "int"{
							dataStr[i][key] = int64(0)
						}
					} else {
						if typeV == "int" {
							dataStr[i][key] = int64(dataStr[i][key].(float64))
						}
					}
				}
			}
			fieldList := []string{}
			for field, _ := range fieldMaps  {
				fieldList = append(fieldList, field)
			}


			buf.WriteString("truncate table " )
			buf.WriteString(tableName)
			buf.WriteString(";\n")

			buf.WriteString("insert into ")
			buf.WriteString(tableName)
			buf.WriteString(" (")
			needDot := false
			for _, field := range fieldList {
				if needDot {
					buf.WriteString(",")
				} else {
					needDot = true
				}
				buf.WriteString("`")
				buf.WriteString(field)
				buf.WriteString("`")
			}
			buf.WriteString(") ")
			buf.WriteString(" values ")
			needDot = false
			for _, row := range dataStr  {
				if needDot {
					buf.WriteString(",")
				} else {
					needDot = true
				}
				needDot2 := false
				buf.WriteString("(")
				for _, field := range fieldList {
					if needDot2 {
						buf.WriteString(",")
					} else {
						needDot2 = true
					}
					value := row[field]
					if value != nil {
						switch value.(type) {
						case string:
							buf.WriteString("'")
							buf.WriteString(value.(string))
							buf.WriteString("'")
						case int64:
							buf.WriteString( strconv.FormatInt(value.(int64), 10))
						}
					}
				}
				buf.WriteString(") ")
			}
			buf.WriteString(";\n")
		}
		return nil
	})
	ioutil.WriteFile(*output,[]byte(buf.Bytes()), 0666)
	if err != nil {
		fmt.Printf("filepath.Walk() returned %v\n", err)
	}
}