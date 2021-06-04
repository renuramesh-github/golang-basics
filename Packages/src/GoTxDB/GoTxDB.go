/**
 * -------------------------------------------------------------------------------
 * @author Dileep
 * Copyright 2018 The Epixelsolutions.pvt.ltd. All rights reserved.
 *
 * Main package to handle the database related connections
 * -------------------------------------------------------------------------------
 */
package GoTxDB

import (
	"GoDB"
	// "Settings"
	"database/sql"
	"encoding/hex"
	"errors"

	"fmt"

	// "fmt"

	_ "mysql"
	// "os/exec"
	"phpserialize"
	"sort"
	"strconv"
	// "strings"
	"reflect"
	"time"
)

//Database object creation
var (
	DBCon *sql.DB
)

/**
 *  Get GetTxdbc is used to connect the DB
 *  @param none
 *  @return DBobject of type sql.DB
 *  @return Txobject of type sql.Tx
 */
func GetTxdbc() (*sql.DB, *sql.Tx, error) {
	DBCon, err := create_connection(GoDB.Gethost(), GoDB.Getport(), GoDB.Getdb(), GoDB.Getuser(), GoDB.Getpass())
	DBCon.SetMaxOpenConns(5)
	DBCon.SetMaxIdleConns(3)
	DBCon.SetConnMaxLifetime(time.Second * 1)
	Tx, err := DBCon.Begin()
	return DBCon, Tx, err
}

/**
 * private function create_connection is used to create the connection
 * @param Host string
 * @param Port string
 * @param dbname string
 * @param username string
 * @param password string
 * @return
 *        DBobject of type sql.Tx
 *        error
 */
func create_connection(Host string, Port string, dbname string, username string, password string) (*sql.DB, error) {
	db, err := sql.Open("mysql", username+":"+password+"@tcp("+Host+")/"+dbname)
	if err != nil {
		return db, err
	}

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		db.Close()
		return db, err
	}

	return db, nil
}

/**
 * db transaction implementation - scr
 *
 */
func CommitTrans(tx *sql.Tx) {
	tx.Commit()
}

func RollbackTrans(tx *sql.Tx) {
	tx.Rollback()
}

/**
 * Function FetchField is used to fetch the field value
 * @param Tx   *sql.Tx
 * @param table string table name
 * @param field String field name
 * @param conditions []string condition string
 * @return field string
 */
func FetchField(tx *sql.Tx, table string, field string, conditions string) (string, error) {
	sqlStatement := "SELECT " + field + " FROM " + table + " WHERE " + conditions + ";"
	var val string
	// Replace 3 with an ID from your database or another random
	// value to test the no rows use case.
	// fmt.Println("FetchField", sqlStatement)

	// fmt.Println(sqlStatement)
	row := tx.QueryRow(sqlStatement)

	switch err := row.Scan(&val); err {
	case sql.ErrNoRows:
		return "", errors.New("error: Rows are empty")
	case nil:
		return val, nil
	default:
		return "", err
	}

}

/**
 * Function FetchField is used to fetch the field value
 * @param Tx   *sql.Tx
 * @param table string table name
 * @param field String field name
 * @param conditions []string condition string
 * @return field string
 */
func VariableGet(tx *sql.Tx, name string) (string, error) {
	sqlStatement := `SELECT value FROM variable WHERE name=?;`
	var val []byte
	// Replace 3 with an ID from your database or another random
	// value to test the no rows use case.
	stmt, err := tx.Prepare(sqlStatement)
	if err != nil {
		return "", err
	}

	err = stmt.QueryRow(name).Scan(&val)
	if err != nil {
		return "", err
	}

	var Rstr string
	err = phpserialize.Unmarshal(val, &Rstr)
	if err != nil {
		return "", err
	}

	return Rstr, err

}

func VariableSet(tx *sql.Tx, name string, value string) (bool, error) {
	count, _ := GoRowCount(tx, "variable", "name = '"+name+"'")
	out, _ := phpserialize.Marshal(value, nil)
	hexS := hex.EncodeToString(out)
	var status bool
	var err error
	var sqlStmt string
	if count > 0 {

		sqlStmt = "UPDATE variable SET value =  x'" + hexS + "' WHERE name = '" + name + "'"

		stmt, _ := tx.Prepare(sqlStmt)

		defer stmt.Close()
		_, err = stmt.Exec()

		status = true
		if err != nil {
			status = false
		}

	} else {

		sqlStmt = "INSERT INTO variable (name,value) VALUES ('" + name + "',x'" + hexS + "')"
		stmt, _ := tx.Prepare(sqlStmt)

		defer stmt.Close()
		_, err = stmt.Exec()

		status = true
		if err != nil {
			status = false
		}
	}

	return status, err
}

/**
 * Function FetchField is used to fetch the field value
 * @param Tx   *sql.Tx
 * @param table string table name
 * @param field String field name
 * @param conditions []string condition string
 * @return field string
 */
func AFLVariableGet(tx *sql.Tx, name string) (string, error) {
	sqlStatement := `SELECT value FROM afl_variable WHERE name=?;`
	var val []byte
	// Replace 3 with an ID from your database or another random
	// value to test the no rows use case.
	stmt, err := tx.Prepare(sqlStatement)
	if err != nil {
		return "", err
	}

	err = stmt.QueryRow(name).Scan(&val)
	if err != nil {
		return "", err
	}
	var Rstr string
	err = phpserialize.Unmarshal(val, &Rstr)
	if err != nil {
		return "", err
	}

	return Rstr, err

}

func AFLVariableSet(tx *sql.Tx, name string, value string) (bool, error) {
	count, _ := GoRowCount(tx, "afl_variable", "name = '"+name+"'")
	out, _ := phpserialize.Marshal(value, nil)
	hexS := hex.EncodeToString(out)
	var status bool
	var err error
	var sqlStmt string
	if count > 0 {

		sqlStmt = "UPDATE afl_variable SET value =  x'" + hexS + "' WHERE name = '" + name + "'"

		stmt, _ := tx.Prepare(sqlStmt)

		defer stmt.Close()
		_, err = stmt.Exec()

		status = true
		if err != nil {
			status = false
		}

	} else {

		sqlStmt = "INSERT INTO afl_variable (name,value) VALUES ('" + name + "',x'" + hexS + "')"
		stmt, _ := tx.Prepare(sqlStmt)

		defer stmt.Close()
		_, err = stmt.Exec()

		status = true
		if err != nil {
			status = false
		}
	}

	return status, err
}

/*
 -----------------------------------------------------------
 - remove quotes around a string
 -----------------------------------------------------------
*/
func Remove_quotes_around(variable string) string {
	if len(variable) > 0 && variable[0] == '"' {
		variable = variable[1:]
	}
	if len(variable) > 0 && variable[len(variable)-1] == '"' {
		variable = variable[:len(variable)-1]
	}

	return variable
}

func unseriliaze(data []byte) (string, error) {
	var in string
	err := phpserialize.Unmarshal(data, &in)
	if err != nil {
		return "", err
	}
	return in, nil
}

func GoRowCount(tx *sql.Tx, tb string, cond string) (count int, errR error) {
	sqlStmt := "SELECT COUNT(*) as count FROM  " + tb
	if len(cond) > 0 {
		sqlStmt += " where " + cond
	}
	// fmt.Println(sqlStmt)
	rows, err := tx.Query(sqlStmt)
	if err != nil {
		return 0, err
	}

	for rows.Next() {
		errR = rows.Scan(&count)
		if errR != nil {
			return 0, errR
		}
	}

	if err != nil {
		return 0, err
	}
	return count, nil
}

func FetchAssoc(tx *sql.Tx, tb string, fields string, join string, cond string) (rData map[string]string, errR error) {
	sqlStmt := "SELECT "

	if len(fields) > 0 {
		sqlStmt += fields
	} else {
		sqlStmt += " * "
	}

	sqlStmt += " FROM " + tb + " "

	if len(join) > 0 {
		sqlStmt += join
	}

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}
	// fmt.Println(sqlStmt)
	rows, err := tx.Query(sqlStmt)

	if err != nil {
		return make(map[string]string, 0), err
	}

	cols, err := rows.Columns()

	if err != nil {
		return make(map[string]string, 0), err
	}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))

	result := make(map[string]string, len(cols))

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return make(map[string]string, 0), err
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[cols[i]] = ""
			} else {
				result[cols[i]] = string(raw)
			}
		}
	}
	return result, nil

}

func GoInsert(tx *sql.Tx, tb string, fields map[string]string) (status bool, errR error) {

	sqlStmt := "INSERT INTO " + tb + " ("
	valueStr := "("
	count := len(fields)
	var i int

	var valArr []interface{}
	for key, val := range fields {
		valArr = append(valArr, val)
		if i++; i < count {
			sqlStmt += key + ","
			valueStr += "?,"
		} else {
			sqlStmt += key
			valueStr += "?"
		}
	}

	sqlStmt += ") "
	valueStr += ")"

	sqlStmt += " VALUES " + valueStr

	stmt, err := tx.Prepare(sqlStmt)

	if err != nil {
		return false, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(valArr...)

	//fmt.Println(err)
	//fmt.Println(res)
	if res != nil {
		return true, nil
	} else {
		return false, err
	}

}

func GoUpdate(tx *sql.Tx, tb string, fields map[string]string, cond string, expr string) (status bool, errR error) {
	sqlStmt := "UPDATE " + tb + " "
	sqlStmt += " SET "
	count := len(fields)
	var i int
	var valArr []interface{}
	if count > 0 {
		for key, val := range fields {
			valArr = append(valArr, val)
			if i++; i < count {
				sqlStmt += key + " = ?, "
			} else {
				sqlStmt += key + " = ? "
			}
		}
	}

	if len(expr) > 0 {
		sqlStmt += expr
	}

	// if len(expr) > 0 {
	// 	if count > 0 {
	// 		sqlStmt += expr
	// 	} else {
	// 		sqlStmt += expr
	// 	}
	// }

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}
	fmt.Println(sqlStmt)
	stmt, err := tx.Prepare(sqlStmt)

	if err != nil {
		return false, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(valArr...)

	if res != nil {
		return true, nil
	} else {
		return false, err
	}
}

func GoDelete(tx *sql.Tx, tb string, cond string) (status bool, errR error) {
	sqlStmt := "DELETE FROM " + tb + " "

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}

	stmt, err := tx.Prepare(sqlStmt)
	if err != nil {
		return false, err
	}
	defer stmt.Close()
	res, err := stmt.Exec()

	if res != nil {
		return true, nil
	} else {
		return false, err
	}
}

func FetchAll(tx *sql.Tx, tb string, fields string, join string, cond string, offset string, limit string) (rData map[int]map[string]string, errR error) {

	sqlStmt := "SELECT "

	if len(fields) > 0 {
		sqlStmt += fields
	} else {
		sqlStmt += " * "
	}

	sqlStmt += " FROM " + tb + " "

	if len(join) > 0 {
		sqlStmt += join
	}

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}

	if len(offset) <= 0 {
		offset = "0"
	}

	if len(limit) > 0 {
		sqlStmt += " limit " + offset + "," + limit
	}
	fmt.Println(sqlStmt)
	rows, err := tx.Query(sqlStmt)

	if err != nil {
		return make(map[int]map[string]string, 0), err
	}

	cols, err := rows.Columns()

	if err != nil {
		return make(map[int]map[string]string, 0), err
	}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))

	result := make(map[int]map[string]string)

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}
	var j int
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return make(map[int]map[string]string, 0), err
		}
		j++
		result[j] = make(map[string]string)
		for i, raw := range rawResult {

			if raw == nil {
				result[j][cols[i]] = ""
			} else {
				result[j][cols[i]] = string(raw)
			}
		}
	}

	return result, nil

}

func FetchAllOrder(tx *sql.Tx, tb string, fields string, join string, cond string, offset string, limit string, order string) (rData map[int]map[string]string, errR error) {

	sqlStmt := "SELECT "

	if len(fields) > 0 {
		sqlStmt += fields
	} else {
		sqlStmt += " * "
	}

	sqlStmt += " FROM " + tb + " "

	if len(join) > 0 {
		sqlStmt += join
	}

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}

	if len(order) > 0 {
		sqlStmt += " ORDER BY " + order
	}

	if len(offset) <= 0 {
		offset = "0"
	}

	if len(limit) > 0 {
		sqlStmt += " limit " + offset + "," + limit
	}

	fmt.Println("FetchAllOrder", sqlStmt)

	rows, err := tx.Query(sqlStmt)

	if err != nil {
		return make(map[int]map[string]string, 0), err
	}

	cols, err := rows.Columns()

	if err != nil {
		return make(map[int]map[string]string, 0), err
	}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))

	result := make(map[int]map[string]string)

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}
	var j int
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return make(map[int]map[string]string, 0), err
		}

		result[j] = make(map[string]string)
		for i, raw := range rawResult {

			if raw == nil {
				result[j][cols[i]] = ""
			} else {
				result[j][cols[i]] = string(raw)
			}
		}

		j++
	}

	var keys []int
	for k := range result {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	resultD := make(map[int]map[string]string, len(keys))
	for v, k := range keys {
		resultD[v] = result[k]
	}

	return resultD, nil

}

func FetchAllOrderGroup(tx *sql.Tx, tb string, fields string, join string, cond string, offset string, limit string, order string, group string) (rData map[int]map[string]string, errR error) {

	sqlStmt := "SELECT "

	if len(fields) > 0 {
		sqlStmt += fields
	} else {
		sqlStmt += " * "
	}

	sqlStmt += " FROM " + tb + " "

	if len(join) > 0 {
		sqlStmt += join
	}

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}

	if len(group) > 0 {
		sqlStmt += " GROUP BY " + group
	}

	if len(order) > 0 {
		sqlStmt += " ORDER BY " + order
	}

	if len(offset) <= 0 {
		offset = "0"
	}

	if len(limit) > 0 {
		sqlStmt += " limit " + offset + "," + limit
	}

	fmt.Println("FetchAllOrderGroup", sqlStmt)

	rows, err := tx.Query(sqlStmt)

	if err != nil {
		return make(map[int]map[string]string, 0), err
	}

	cols, err := rows.Columns()

	if err != nil {
		return make(map[int]map[string]string, 0), err
	}

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))

	result := make(map[int]map[string]string)

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}
	var j int
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return make(map[int]map[string]string, 0), err
		}

		result[j] = make(map[string]string)
		for i, raw := range rawResult {

			if raw == nil {
				result[j][cols[i]] = ""
			} else {
				result[j][cols[i]] = string(raw)
			}
		}

		j++
	}
	var keys []int
	for k := range result {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	resultD := make(map[int]map[string]string, len(keys))
	for v, k := range keys {
		resultD[v] = result[k]
	}

	return resultD, nil

}

func FetchCol(tx *sql.Tx, tb string, fields string, join string, cond string) (rData []string, errR error) {
	sqlStmt := "SELECT "

	if len(fields) > 0 {
		sqlStmt += fields
	} else {
		sqlStmt += " * "
	}

	sqlStmt += " FROM " + tb + " "

	if len(join) > 0 {
		sqlStmt += join
	}

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}

	// fmt.Println(sqlStmt)

	rows, err := tx.Query(sqlStmt)
	var fieldD string

	if err != nil {
		return make([]string, 0), err
	}

	// Result is your slice string.
	var j int
	for rows.Next() {
		err = rows.Scan(&fieldD)
		if err != nil {
			return make([]string, 0), err
		}
		j++
		rData = append(rData, fieldD)

	}
	return rData, nil

}

func FetchColOrder(tx *sql.Tx, tb string, fields string, join string, cond string, order string) (rData []string, errR error) {
	sqlStmt := "SELECT "

	if len(fields) > 0 {
		sqlStmt += fields
	} else {
		sqlStmt += " * "
	}

	sqlStmt += " FROM " + tb + " "

	if len(join) > 0 {
		sqlStmt += join
	}

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}

	if len(order) > 0 {
		sqlStmt += " ORDER BY " + order
	}

	// fmt.Println(sqlStmt)

	rows, err := tx.Query(sqlStmt)
	var fieldD string

	if err != nil {
		return make([]string, 0), err
	}

	// Result is your slice string.
	var j int
	for rows.Next() {
		err = rows.Scan(&fieldD)
		if err != nil {
			return make([]string, 0), err
		}
		j++
		rData = append(rData, fieldD)

	}
	return rData, nil

}

func GoMultiInsert(tx *sql.Tx, tb string, fields map[int]map[string]string) (status bool, errR error) {
	sqlStmt := "INSERT INTO " + tb + " ("
	var valArr []interface{}
	var c int
	valueStr := ""

	for _, Data := range fields {
		count := len(Data)
		var i int
		i = 0
		valueStr += "("
		keys := make([]string, 0, len(Data))
		for k := range Data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			valArr = append(valArr, Data[k])
			if i++; i < count {
				if c == 0 {
					sqlStmt += k + ","
				}
				valueStr += "?,"
			} else {
				if c == 0 {
					sqlStmt += k
				}
				valueStr += "?"
			}
		}
		valueStr += "),"
		c++
	}
	sqlStmt += ") "
	valueStr = valueStr[0 : len(valueStr)-1]

	sqlStmt += " VALUES " + valueStr
	// fmt.Println(sqlStmt)

	stmt, err := tx.Prepare(sqlStmt)
	if err != nil {
		return false, err
	}
	defer stmt.Close()
	//fmt.Println(valArr)
	res, err := stmt.Exec(valArr...)

	if res != nil {
		return true, nil
	} else {
		return false, err
	}

}

func GoMultiUpdate(tx *sql.Tx, tb string, fields map[int]map[string]string, id string, cond string, expr string) (status bool, errR error) {

	sqlStmt := "UPDATE " + tb + " "
	sqlStmt += "SET "

	userArrS := "("

	var valArr, valArr2 []interface{}
	var field_name map[string]string
	field_name = make(map[string]string)
	var i, k int
	count := len(fields)

	// --------- sorting key of field[][key][] ---------------
	var columns []string
	for _, field := range fields {

		first := 1
		for ky := range field {
			columns = append(columns, ky)
		}
		if first == 1 {
			break
		}
	}
	sort.Strings(columns)

	// --------------------------------------------------------

	for key, value := range fields {
		keyS := strconv.Itoa(key)
		if i++; i < count {
			userArrS += "'" + keyS + "', "
		} else {
			userArrS += "'" + keyS + "' "
		}

		for _, column := range columns {
			field_name[column] += "WHEN " + id + " = '" + keyS + "' THEN ? "
			valArr = append(valArr, value[column])

		}
	}

	userArrS += ")"
	// fmt.Println(userArrS)

	count1 := len(field_name)

	// --------- sorting valArr ---------------
	for i := 0; i < len(field_name); i++ {
		for j := i; j < len(valArr); j += count1 {
			valArr2 = append(valArr2, valArr[j])
		}
	}
	valArr = valArr2
	// --------------------------------------------------------

	for _, column := range columns {
		if k++; k < count1 {
			sqlStmt += column + " = (CASE " + field_name[column] + "END), "
		} else {
			sqlStmt += column + " = (CASE " + field_name[column] + "END) "
		}

	}
	if len(expr) > 0 {
		sqlStmt += expr
	}
	sqlStmt += " WHERE " + id + " IN " + userArrS
	if len(cond) > 0 {
		sqlStmt += " AND " + cond
	}

	fmt.Println(valArr)
	fmt.Println("sqlStmt ::::::: ", sqlStmt)
	stmt, err := tx.Prepare(sqlStmt)

	if err != nil {
		return false, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(valArr...)

	if res != nil {
		return true, nil
	} else {
		return false, err
	}
}

func GoMultiUpdateCommon(tx *sql.Tx, tb string, fields map[string][]string, cond string, expr string, casecond []string) (status bool, errR error) {
	sqlStmt := "UPDATE " + tb + " "
	sqlStmt += " SET "
	count := len(fields)
	var i int

	var valArr []interface{}
	if count > 0 {
		for key, val := range fields {
			// 		// valArr = append(valArr, val)

			sqlStmt += key + "=(case"
			if i++; i < count {
				for j := 0; j < len(casecond); j++ {
					if (casecond[j]) == "" {
						break
					}
					valArr = append(valArr, val[j])
					sqlStmt += "  when " + casecond[j] + " then ? "
				}
				sqlStmt += " end),"
			} else {
				for j := 0; j < len(casecond); j++ {
					if casecond[j] == "" {
						break
					}
					valArr = append(valArr, val[j])
					sqlStmt += "  when " + casecond[j] + " then ?"
				}
				sqlStmt += " end)"
			}
		}
	}

	if len(expr) > 0 {
		sqlStmt += expr
	}

	if len(cond) > 0 {
		sqlStmt += " WHERE " + cond
	}

	// fmt.Println("Up", sqlStmt)

	stmt, err := tx.Prepare(sqlStmt)
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(valArr...)

	if res != nil {
		return true, nil
	} else {
		return false, err
	}

}
func GoMultipleRawUpdate(tx *sql.Tx, tb string, fields map[string]interface{}, cond string) (status bool, errR error) {

	// domine := db.GetDomine()
	sqlStmt := "UPDATE " + tb + " "
	var valArr []interface{}

	sqlStmt += " SET "
	var i int
	length := len(fields)
	// fmt.Println("length------------>", length)
	for key, val := range fields {
		i++
		sqlStmt += fmt.Sprintf(" %v = ", key)
		value := reflect.ValueOf(val)
		if value.Kind() == reflect.Map {
			newvl := make(map[string]interface{}, 0)
			newvl = val.(map[string]interface{})
			sqlStmt += fmt.Sprintf(" CASE ")

			new_fields := newvl["fields"].(map[string]interface{})
			for key1, val1 := range new_fields {
				sqlStmt += fmt.Sprintf(" WHEN %v THEN %v", key1, val1)
			}
			sqlStmt += fmt.Sprintf(" ELSE  %s ", key)
			// fmt.Println("length------------>", i, length)
			if i < length {
				sqlStmt += " END ,"
			} else {
				sqlStmt += " END "
			}

		} else {
			sqlStmt += fmt.Sprintf(" %v ", val)
		}

	}

	// sqlStmt += " WHERE currency_id IN " + cond

	if len(cond) > 0 {
		// fmt.Println("cond", cond)
		sqlStmt += " WHERE " + cond
	}
	stmt, err := tx.Prepare(sqlStmt)
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(valArr...)

	if res != nil {
		return true, nil
	} else {
		return false, err
	}
}
