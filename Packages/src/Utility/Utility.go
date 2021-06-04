/**
 * -------------------------------------------------------------------------------
 * @author Dileep
 * Copyright 2018 The Epixelsolutions.pvt.ltd. All rights reserved.
 *
 * Import the commong package for the application avoid the duplication of the package
 *  Loading
 * -------------------------------------------------------------------------------
 */

package Utility

import (
	E "ErrorHandler/PanicError"
	"GoDB"
	"Settings"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	// "os"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	PR = fmt.Println
	// PR = TestK
	PS = fmt.Sprintf
)

// func TestK(...interface{}){
// 	return
// }

func CheckError(db *sql.DB, err error, eX bool, log bool, callback string) {
	if err != nil {
		//error logs comes here

		if eX {
			//E.Error_log(db,err)
			data := make(map[string]string, 5)
			data["particulars"] = "cron"
			data["count"] = "0"
			data["category"] = "cron error"
			data["lock_status"] = "0"
			date := Epsdate(db)
			dateI := int(date)
			dateStr := strconv.Itoa(dateI)
			data["created"] = dateStr
			extra := "Error : " + err.Error()

			ct := 0
			for {
				_, errD := Master_Table_error_Q(db, data, extra)
				if errD == nil {
					break
				}

				if ct++; ct > 5 {
					//accidential lookup
					break
				}
			}

		}

		if log {
			//log comes here
			E.Error_log(db, err)
		}

	}
}

/**
 * Function Epsdate is used to get the current timestamp
 * @param db *sql.DB db connection
 * @return Int Unix time stamp
 * Step1 : Check the testing mode
 * Step2 : IF yes then return the testing timestamp
 * Step3 : Get the current time stamp from the server according to the location
 */

func dateFormat(dtstr string) string {
	dt, _ := time.Parse(Settings.Site.SitePublished, dtstr)
	return dt.Format(Settings.Site.SitePublished)
}

func EpsdateDev(db *sql.DB) int64 {

	TEXTMODE, _ := GoDB.AFLVariableGet(db, "afl_enable_test_mode")
	//CheckError(db, err, true, true, "")
	TEXTDATE, _ := GoDB.AFLVariableGet(db, "afl_enable_test_date")
	//CheckError(db, err, true, true, "")

	CRONSTOPMODE, _ := GoDB.VariableGet(db, "stop_cron")
	mode_status := StringStatusCheck(CRONSTOPMODE)

	if mode_status {
		CRONSTOPMODE = "1"
	} else {
		CRONSTOPMODE = "0"
	}

	//CheckError(db, err, true, true, "")
	CRONSTOPDATE, _ := GoDB.VariableGet(db, "afl_enable_test_cron_date")
	mode_status_date := StringStatusCheck(CRONSTOPDATE)
	if mode_status_date {
		CRONSTOPDATE = "1"
	} else {
		CRONSTOPDATE = "0"
	}
	// PR("Cron status", CRONSTOPMODE, CRONSTOPDATE)
	//CheckError(db, err, true, true, "")

	/*
	 * -----------------------------------------
	 * Get current system date
	 * -----------------------------------------
	 */

	var unix_time_stamp int64
	/*if CRONSTOPMODE == "1" && CRONSTOPDATE == "1" {

		cron_timestamp, _ := GoDB.VariableGet(db, "afl_cron_mode_date")
		// PR("Cron tim1", cron_timestamp)
		unix_time_stamp, _ = strconv.ParseInt(cron_timestamp, 10, 64)

	} else */if TEXTMODE == "1" && TEXTDATE == "1" {

		current_date, _ := GoDB.AFLVariableGet(db, "afl_testing_date")
		current_date = current_date + ":00"
		current_date = dateFormat(current_date)
		unix_time_stamp = StrtotimeDev(current_date)

	} else {
		TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
		location, _ := time.LoadLocation(TIMEZONE)
		//CheckError(db, err, true, true, "")
		t := time.Now().In(location)
		date := t.Format("2006-01-02 15:04:05")
		// PR(date)
		cmd := exec.Command("date", "-d", date, `+%s`)
		var out bytes.Buffer
		var stderr bytes.Buffer
		out.Reset()
		cmd.Stdout = &out
		cmd.Stderr = &stderr

		errC := cmd.Run()
		if errC != nil {
			fmt.Println("ERROR C", errC)
			unix_time_stamp = t.Unix()
		}

		s := out.String()
		s = strings.TrimSpace(s)
		unix_time_stamp, _ = strconv.ParseInt(s, 10, 64)

		//set timezone,

		// PR("U", unix_time_stamp, time.Now().UTC().Unix())
	}

	/*t.String()
	current_date := (t.Format(Settings.Site.SitePublished))

	if TEXTMODE == "1" && TEXTDATE == "1" {
		current_date, _ = GoDB.AFLVariableGet(db, "afl_testing_date")
		current_date = current_date + ":00"
		current_date = dateFormat(current_date)

	}

	// First, we create an instance of a timezone location object
	loc, _ := time.LoadLocation(TIMEZONE)

	// this is our custom format. Note that the format must point to this exact time
	format := Settings.Site.SitePublished

	// this is your timestamp
	timestamp := current_date

	system_time, err := time.ParseInLocation(format, timestamp, loc)
	*/

	return unix_time_stamp

}

func Epsdate(db *sql.DB) int64 {

	TEXTMODE, _ := GoDB.AFLVariableGet(db, "afl_enable_test_mode")
	//CheckError(db, err, true, true, "")
	TEXTDATE, _ := GoDB.AFLVariableGet(db, "afl_enable_test_date")
	//CheckError(db, err, true, true, "")

	CRONSTOPMODE, _ := GoDB.VariableGet(db, "stop_cron")
	mode_status := StringStatusCheck(CRONSTOPMODE)

	if mode_status {
		CRONSTOPMODE = "1"
	} else {
		CRONSTOPMODE = "0"
	}

	//CheckError(db, err, true, true, "")
	CRONSTOPDATE, _ := GoDB.VariableGet(db, "afl_enable_test_cron_date")
	mode_status_date := StringStatusCheck(CRONSTOPDATE)
	if mode_status_date {
		CRONSTOPDATE = "1"
	} else {
		CRONSTOPDATE = "0"
	}
	// PR("Cron status", CRONSTOPMODE, CRONSTOPDATE)
	//CheckError(db, err, true, true, "")

	/*
	 * -----------------------------------------
	 * Get current system date
	 * -----------------------------------------
	 */

	var unix_time_stamp int64
	// if CRONSTOPMODE == "1" && CRONSTOPDATE == "1" {

	// 	cron_timestamp, _ := GoDB.VariableGet(db, "afl_cron_mode_date")
	// 	// PR("Cron tim1", cron_timestamp)
	// 	unix_time_stamp, _ = strconv.ParseInt(cron_timestamp, 10, 64)

	// } else
	if TEXTMODE == "1" && TEXTDATE == "1" {

		current_date, _ := GoDB.AFLVariableGet(db, "afl_testing_date")
		current_date = current_date + ":00"
		current_date = dateFormat(current_date)
		unix_time_stamp = Strtotime(db, current_date)

	} else {
		TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
		location, _ := time.LoadLocation(TIMEZONE)
		//CheckError(db, err, true, true, "")
		t := time.Now().In(location)
		t.String()
		current_date := (t.Format(Settings.Site.SitePublished))

		if TEXTMODE == "1" && TEXTDATE == "1" {
			current_date, _ = GoDB.AFLVariableGet(db, "afl_testing_date")
			current_date = current_date + ":00"
			current_date = dateFormat(current_date)

		}

		// First, we create an instance of a timezone location object
		loc, _ := time.LoadLocation(TIMEZONE)

		// this is our custom format. Note that the format must point to this exact time
		format := Settings.Site.SitePublished

		// this is your timestamp
		timestamp := current_date

		system_time, _ := time.ParseInLocation(format, timestamp, loc)
		unix_time_stamp = system_time.Unix()
		//set timezone,

		// PR("U", unix_time_stamp, time.Now().UTC().Unix())
	}

	/*t.String()
	current_date := (t.Format(Settings.Site.SitePublished))

	if TEXTMODE == "1" && TEXTDATE == "1" {
		current_date, _ = GoDB.AFLVariableGet(db, "afl_testing_date")
		current_date = current_date + ":00"
		current_date = dateFormat(current_date)

	}

	// First, we create an instance of a timezone location object
	loc, _ := time.LoadLocation(TIMEZONE)

	// this is our custom format. Note that the format must point to this exact time
	format := Settings.Site.SitePublished

	// this is your timestamp
	timestamp := current_date

	system_time, err := time.ParseInLocation(format, timestamp, loc)
	*/

	return unix_time_stamp

}

func Strtotime(db *sql.DB, str string) int64 {

	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")

	loc, _ := time.LoadLocation(TIMEZONE)

	// this is our custom format. Note that the format must point to this exact time
	format := Settings.Site.SitePublished

	// this is your timestamp
	timestamp := str

	system_time, _ := time.ParseInLocation(format, timestamp, loc)

	/*date := t.Format("2006-01-02 15:04:05")
	cmd := exec.Command("date", "-d", date, `+%s`)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	errC := cmd.Run()
	if errC != nil {
		return t.Unix()
	}

	s := out.String()
	s = strings.TrimSpace(s)
	unix_time_stamp, _ := strconv.ParseInt(s, 10, 64)*/

	return system_time.Unix()
}

func StrtotimeDev(str string) int64 {
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, str)

	if err != nil {
		return 0
	}
	date := t.Format("2006-01-02 15:04:05")
	cmd := exec.Command("date", "-d", date, `+%s`)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	errC := cmd.Run()
	if errC != nil {
		return t.Unix()
	}

	s := out.String()
	s = strings.TrimSpace(s)
	unix_time_stamp, _ := strconv.ParseInt(s, 10, 64)

	return unix_time_stamp
}

/*
 * -----------------------------------------------------------------------
 * Split the given timestamp
 * -----------------------------------------------------------------------
 */
func Go_date_splits(db *sql.DB, timestamp int64) map[string]int {

	delta := EpsdateDev(db) - Epsdate(db)
	timestamp += delta

	date_splits := make(map[string]int)
	currentTime := time.Unix(timestamp, 0)

	fmt.Println(" -------currentTime -----------", currentTime, int(currentTime.Weekday()))
	timeStampString := currentTime.Format(Settings.Site.SitePublished)

	layOut := Settings.Site.SitePublished
	AcctimeStamp, err := time.Parse(layOut, timeStampString)

	if err != nil {
		fmt.Println(err)
	}
	hr, min, sec := AcctimeStamp.Clock()
	_, week := currentTime.ISOWeek()

	date_splits["y"] = currentTime.Year()
	date_splits["m"] = int(currentTime.Month())
	date_splits["d"] = currentTime.Day()
	date_splits["h"] = hr
	date_splits["min"] = min
	date_splits["s"] = sec
	date_splits["aw"] = week

	if date_splits["d"] <= 15 {
		date_splits["sm"] = 1
	} else {
		date_splits["sm"] = 2
	}
	//find out the week from business startting day
	diff := (Business_starting_day_difference(db, timestamp))
	ti := int(currentTime.Weekday())
	s := diff.Seconds()
	date_splits["wd"] = ti
	if ti == 1 {
		s += 24 * 60 * 60
	}
	date_splits["w"] = int(math.Ceil(s / (7 * 24 * 60 * 60)))
	return date_splits
}

/*
 * -----------------------------------------------------------------------
 * Date difference
 * -----------------------------------------------------------------------
 */
func Business_starting_day_difference(db *sql.DB, timestamp int64) time.Duration {
	system_date_time_stamp := timestamp

	system_date := time.Unix(system_date_time_stamp, 0)

	_, _ = system_date.Zone()

	//businss start date
	var business_start_date string

	business_start_date = Settings.Site.SitePublished
	business_start_date, _ = GoDB.AFLVariableGet(db, "afl_business_starting_date")
	business_start_date += ":00"

	timeFormat := Settings.Site.SitePublished
	business_start_date_time, _ := time.Parse(timeFormat, business_start_date)
	//CheckError(db, err, true, true, "")

	diff := system_date.Sub(business_start_date_time)

	return diff
}

func EpsRoot(db *sql.DB) int {
	UID, _ := GoDB.AFLVariableGet(db, "afl_genealogy_root_user")

	if UID == "" || len(UID) <= 0 {
		UID = "3"
	}

	RUID, _ := strconv.Atoi(UID)
	return RUID
}

func EpsUserInfo(db *sql.DB, uid int) (Uinfo map[string]string, err error) {
	uidStr := strconv.Itoa(uid)
	Uinfo, err = GoDB.FetchAssoc(
		db,
		"afl_user_genealogy",
		"afl_user_genealogy.*,users.name",
		"LEFT JOIN users ON users.uid = afl_user_genealogy.uid",
		"afl_user_genealogy.uid = "+uidStr,
	)

	if len(Uinfo) <= 0 {
		return make(map[string]string, 0), nil
	}

	return Uinfo, nil
}

func Ordinalize(num int) string {

	var ordinalDictionary = map[int]string{
		0: "th",
		1: "st",
		2: "nd",
		3: "rd",
		4: "th",
		5: "th",
		6: "th",
		7: "th",
		8: "th",
		9: "th",
	}

	// math.Abs() is to convert negative number to positive
	floatNum := math.Abs(float64(num))
	positiveNum := int(floatNum)

	if ((positiveNum % 100) >= 11) && ((positiveNum % 100) <= 13) {
		return strconv.Itoa(num) + "th"
	}
	return strconv.Itoa(num) + ordinalDictionary[positiveNum]
}

func Go_get_username(db *sql.DB, uid int) string {
	uidStr := strconv.Itoa(uid)
	username, _ := GoDB.FetchField(db, "users", "name", "uid = "+uidStr)
	return username
}

func Go_commerce_order_load(db *sql.DB, order_id int) (order map[string]string, err error) {
	order, err = GoDB.FetchAssoc(
		db,
		"commerce_order",
		"commerce_order.uid,commerce_order.created,commerce_order.order_id,commerce_order.mail,commerce_order.status,afl_purchases.amount_paid,afl_purchases.afl_points,afl_purchases.category,commerce_line_item.line_item_id,field_data_commerce_product.commerce_product_product_id as product_id,field_data_commerce_order_total.commerce_order_total_currency_code as currency",
		"LEFT JOIN afl_purchases ON commerce_order.order_id = afl_purchases.order_id "+
			"LEFT JOIN commerce_line_item ON commerce_line_item.order_id = afl_purchases.order_id "+
			"LEFT JOIN field_data_commerce_product ON field_data_commerce_product.entity_id = commerce_line_item.line_item_id "+
			"LEFT JOIN field_data_commerce_order_total ON field_data_commerce_order_total.entity_id = afl_purchases.order_id "+
			"LEFT JOIN field_data_commerce_line_items ON field_data_commerce_line_items.commerce_line_items_line_item_id = commerce_line_item.line_item_id ",
		" commerce_order.order_id = "+strconv.Itoa(order_id)+" AND commerce_line_item.type = 'product'",
	)

	return
}

func StringStatusCheck(strcheck string) (status bool) {
	if len(strcheck) > 0 &&
		strcheck != "0" &&
		strcheck != "false" &&
		strcheck != "FALSE" &&
		strcheck != "" {
		return true
	} else {
		return false
	}
}

func Go_default_member_rank(db *sql.DB) (rank string) {
	//Get the default rank from the settings
	rank = "0"
	ENABLE_DEMO_STR, _ := GoDB.AFLVariableGet(db, "afl_enable_demo_mode")
	ENABLE_DEMO := StringStatusCheck(ENABLE_DEMO_STR)

	if ENABLE_DEMO == true {
		rank, _ := GoDB.AFLVariableGet(db, "afl_default_rank")
		rankCheck := StringStatusCheck(rank)

		if rankCheck == true {
			return rank
		} else {
			return "0"
		}
	}
	//return the rank to the functions
	return
}

func GetRelativePosition(db *sql.DB, uid int, refer_id int) string {
	Uinfo, _ := EpsUserInfo(db, uid)

	if len(Uinfo) <= 0 {
		return ""
	}

	//check the uid is root user then return null
	AFL_ROOT := EpsRoot(db)
	AFL_ROOT_STR := strconv.Itoa(AFL_ROOT)

	if AFL_ROOT_STR == Uinfo["parent_uid"] {
		return ""
	}
	//check the parent id == refer id then return the position
	refer_uid_str := strconv.Itoa(refer_id)

	if refer_uid_str == Uinfo["parent_uid"] {
		return Uinfo["position"]
	}

	//else recursively call the parent and find the postion
	uid, _ = strconv.Atoi(Uinfo["parent_uid"])
	return GetRelativePosition(db, uid, refer_id)
}

func Auto_position_update(db *sql.DB, Uid int, position string) (status bool) {
	//check uid
	if Uid == 0 {
		return false
	}
	//update the position in genology
	var m map[string]string
	m = make(map[string]string, 1)
	m["auto_position"] = position

	status, _ = GoDB.GoUpdate(db, "afl_user_genealogy", m, "uid = "+strconv.Itoa(Uid), "")

	return

}

func NestedSetUpdate(db *sql.DB, uid int, parent_id int, tb string) (status bool, err error) {
	//uidstr:= strconv.Itoa(uid)
	parentstr := strconv.Itoa(parent_id)
	uidstr := strconv.Itoa(uid)

	newLeftStr, _ := GoDB.FetchField(db, tb, "rgt", "node_id = "+parentstr)
	newLeft, _ := strconv.Atoi(newLeftStr)

	if newLeft > 0 {
		field := make(map[string]string, 0)

		//update right
		status, _ = GoDB.GoUpdate(db, tb, field, "rgt >= "+newLeftStr, "rgt = rgt+2")

		//Update left
		status, _ = GoDB.GoUpdate(db, tb, field, "lft > "+newLeftStr, "lft = lft+2")

		//Insert new node
		data := make(map[string]string, 4)
		data["lft"] = newLeftStr
		data["rgt"] = strconv.Itoa(newLeft + 1)
		data["parent_uid"] = parentstr
		data["node_id"] = uidstr
		status, _ = GoDB.GoInsert(db, tb, data)

	}

	return status, nil
}

func NestedSetDelete(db *sql.DB, uid int, parent_id int, tb string) (status bool, err error) {
	//uidstr:= strconv.Itoa(uid)
	parentstr := strconv.Itoa(parent_id)
	uidstr := strconv.Itoa(uid)

	newLeftStr, _ := GoDB.FetchField(db, tb, "rgt", "node_id = "+parentstr)
	newLeft, _ := strconv.Atoi(newLeftStr)

	if newLeft > 0 {
		field := make(map[string]string, 0)

		//update right
		status, _ = GoDB.GoUpdate(db, tb, field, "rgt >= "+newLeftStr, "rgt = rgt-2")

		//Update left
		status, _ = GoDB.GoUpdate(db, tb, field, "lft > "+newLeftStr, "lft = lft-2")

		//delete node

		status, _ = GoDB.GoDelete(db, tb, "node_id = "+uidstr)

	}

	return status, nil
}

func Order_status_updation(db *sql.DB, order_id int, Instatus string) (status bool, err error) {
	//Check the order exist or not
	order_id_str := strconv.Itoa(order_id)
	order_count, _ := GoDB.GoRowCount(db, "commerce_order", "order_id = "+order_id_str)

	//if yes then update the commerce order status
	if order_count > 0 {
		fields := make(map[string]string, 1)
		fields["status"] = Instatus
		status, err = GoDB.GoUpdate(db, "commerce_order", fields, "order_id = "+order_id_str, "")
	} else {
		status = false
		err = errors.New("Error : order is not found")
	}

	return

}

func Master_Table_Q(db *sql.DB, data map[string]string, extra map[string]string) (status bool, err error) {
	if len(extra) > 0 {
		enEpram, err := json.Marshal(extra)
		if err != nil {
			return false, err
		}
		data["extra_params"] = string(enEpram)
	}
	return GoDB.GoInsert(db, "cron_lock_tb", data)
}
func Master_Table_light_Q(db *sql.DB, data map[string]string, extra map[string]string) (status bool, err error) {
	if len(extra) > 0 {
		enEpram, err := json.Marshal(extra)
		if err != nil {
			return false, err
		}
		data["extra_params"] = string(enEpram)
	}
	return GoDB.GoInsert(db, "cron_lock_light_tb", data)
}

func Master_Table_error_Q(db *sql.DB, data map[string]string, extra string) (status bool, err error) {
	if len(extra) > 0 {
		enEpram, err := json.Marshal(extra)
		if err != nil {
			return false, err
		}
		data["extra_params"] = string(enEpram)
	}
	return GoDB.GoInsert(db, "cron_lock_tb", data)
}

func ChunkArray(dataIn []string, size int) [][]string {
	var divided [][]string

	if len(dataIn) > 0 {
		end := len(dataIn)
		for i := 0; i < end; i += size {
			neWE := i + size
			if neWE >= end {
				neWE = end
			}
			divided = append(divided, dataIn[i:neWE])
		}
	}

	return divided

}

func Array_diff(fA []string, sA []string) []string {
	var diff []string
	var f bool
	for _, v := range fA {
		f = true
		for _, v1 := range sA {
			if v == v1 {
				f = false
				break
			}
		}
		if f == true {
			diff = append(diff, v)
		}
	}
	return diff
}

/**
 * GetParent is used to get the parent from the nested set
 * @param  Uid int Uid for the parent
 */
func GetParents(db *sql.DB, Uid int, tb string) []string {
	uidStr := strconv.Itoa(Uid)
	cond := " node_id = " + uidStr
	node, err := GoDB.FetchAssoc(db, tb, "", "", cond)

	var parents []string
	parents = make([]string, 0)

	if err != nil {
		return parents
	}

	if len(node) > 0 {
		left := node["lft"]
		right := node["rgt"]
		cond = " lft < " + left + " AND rgt > " + right
		parents, _ = GoDB.FetchCol(db, tb, "node_id", "", cond)
	}

	return parents

}

func GetParentsWithOrder(db *sql.DB, Uid int, tb string) []string {
	uidStr := strconv.Itoa(Uid)
	cond := " node_id = " + uidStr
	node, err := GoDB.FetchAssoc(db, tb, "", "", cond)

	var parents []string
	parents = make([]string, 0)

	if err != nil {
		return parents
	}

	if len(node) > 0 {
		left := node["lft"]
		right := node["rgt"]
		cond = " lft < " + left + " AND rgt > " + right
		parents, _ = GoDB.FetchColOrder(db, tb, "node_id", "", cond, "rgt-lft DESC")
	}

	return parents

}

/**
 * GetParentPosition is used to get the parent position
 * @param {[type]} db  *sql.DB [description]
 * @param {[type]} Uid int     [description]
 * @param {[type]} tb  string  [description]
 * @param {[type]} inc bool)   (map[string][]string [description]
 */
func GetParentPosition(db *sql.DB, Uid int, tb string, inc bool) map[string][]string {

	parents := GetParents(db, Uid, tb)

	if inc == true {
		parents = append(parents, strconv.Itoa(Uid))
	}

	Rdata := make(map[string][]string, 0)

	if len(parents) > 0 {

		field_fetch := "parent_uid"
		position_feild := "position"

		if tb == "nested_set_referal" {
			field_fetch = "referrer_uid"
			position_feild = "relative_position"
		}
		cond := " " + position_feild + " = 'LEFT' "
		UidChunk := ChunkArray(parents, 50)
		// PR("chunks", UidChunk)
		Or := " ("
		ChunkCount := len(UidChunk)
		var i int
		for _, val := range UidChunk {
			if i++; i < ChunkCount {
				Or += "uid IN (" + strings.Join(val, ",") + ") OR "
			} else {
				Or += "uid IN (" + strings.Join(val, ",") + ")"
			}
		}
		Or += " ) "
		cond += " AND " + Or
		LEFT_UIDS, _ := GoDB.FetchCol(db, "afl_user_genealogy", field_fetch, "", cond)

		cond = " " + position_feild + " = 'RIGHT' "
		cond += " AND " + Or

		RIGHT_UIDS, _ := GoDB.FetchCol(db, "afl_user_genealogy", field_fetch, "", cond)

		Rdata["LEFT"] = make([]string, 0)
		Rdata["LEFT"] = LEFT_UIDS

		Rdata["RIGHT"] = make([]string, 0)
		Rdata["RIGHT"] = RIGHT_UIDS

	}

	return Rdata
}

func UserLoad(db *sql.DB, uid int) (user map[string]string, err error) {
	user, err = GoDB.FetchAssoc(
		db,
		"users",
		"users.*,field_data_field_afl_sponsor.field_afl_sponsor_target_id as sponser,field_data_field_afl_position.field_afl_position_value as position,field_data_field_mlm_source.field_mlm_source_value",
		"LEFT JOIN field_data_field_afl_sponsor ON field_data_field_afl_sponsor.entity_id = users.uid "+
			"LEFT JOIN field_data_field_afl_position ON field_data_field_afl_position.entity_id = users.uid "+
			"LEFT JOIN field_data_field_mlm_source ON field_data_field_mlm_source.entity_id = users.uid ",
		" users.uid = "+strconv.Itoa(uid),
	)

	return
}

func LockImplementation(db *sql.DB, tb string, unique_id string, status_field string, value string, data map[int]map[string]string) (status bool, err error) {
	// Fetch the unique_id from the table
	var unique_ids []string
	//Update the lock status to the to rows
	if len(data) > 0 {
		for _, val := range data {

			unique_ids = append(unique_ids, val[unique_id])
		}
	}

	if len(unique_ids) > 0 {
		cond := ""
		chunkArr := ChunkArray(unique_ids, 50)
		Or := " ("
		ChunkCount := len(chunkArr)
		var i int
		for _, val := range chunkArr {
			if i++; i < ChunkCount {
				Or += " " + unique_id + " IN (" + strings.Join(val, ",") + ") OR "
			} else {
				Or += " " + unique_id + " IN (" + strings.Join(val, ",") + ")"
			}
		}
		Or += " ) "
		cond += Or

		fields := map[string]string{
			status_field: value,
		}
		status, err = GoDB.GoUpdate(db, tb, fields, cond, "")
		return status, err
	} else {
		return false, nil
	}

	return true, nil

}

func LockImplementationExpr(db *sql.DB, tb string, unique_id string, status_field string, value string, data map[int]map[string]string, expr string) (status bool, err error) {
	afl_date := Epsdate(db)
	afl_date_I := int(afl_date)
	afl_date_str := strconv.Itoa(afl_date_I)
	// Fetch the unique_id from the table
	var unique_ids []string
	//Update the lock status to the to rows
	if len(data) > 0 {
		for _, val := range data {
			unique_ids = append(unique_ids, val[unique_id])
		}
	}

	if len(unique_ids) > 0 {
		cond := ""
		chunkArr := ChunkArray(unique_ids, 50)
		Or := " ("
		ChunkCount := len(chunkArr)
		var i int
		for _, val := range chunkArr {
			if len(val) > 0 {
				if i++; i < ChunkCount {
					Or += " " + unique_id + " IN (" + strings.Join(val, ",") + ") OR "
				} else {
					Or += " " + unique_id + " IN (" + strings.Join(val, ",") + ")"
				}
			}
		}
		Or += " ) "
		cond += Or
		fields := map[string]string{
			status_field:  value,
			"locked_time": afl_date_str,
		}

		status, err = GoDB.GoUpdate(db, tb, fields, cond, expr)

		return status, err
	} else {
		return false, nil
	}

	return true, nil

}

func LockImplementationCond(db *sql.DB, tb string, unique_id string, status_field string, value string, data map[int]map[string]string, condP string) (status bool, err error) {
	// Fetch the unique_id from the table
	var unique_ids []string
	//Update the lock status to the to rows
	if len(data) > 0 {
		for _, val := range data {

			unique_ids = append(unique_ids, val[unique_id])
		}
	}

	if len(unique_ids) > 0 {
		cond := ""
		chunkArr := ChunkArray(unique_ids, 50)
		Or := " ("
		ChunkCount := len(chunkArr)
		var i int
		for _, val := range chunkArr {
			if i++; i < ChunkCount {
				Or += " " + unique_id + " IN (" + strings.Join(val, ",") + ") OR "
			} else {
				Or += " " + unique_id + " IN (" + strings.Join(val, ",") + ")"
			}
		}
		Or += " ) "
		cond += Or

		if len(condP) > 0 {
			cond += condP
		}

		fields := map[string]string{
			status_field: value,
		}
		status, err = GoDB.GoUpdate(db, tb, fields, cond, "")
		return status, err
	} else {
		return false, nil
	}

	return true, nil

}

func TypeOfAssertion(I interface{}) string {
	var s string
	s = ""
	switch I.(type) {
	case float64:
		s = strconv.FormatFloat(I.(float64), 'f', 6, 64)
	case string:
		s = I.(string)

	}

	return s
}

func PreviousCalcDate(db *sql.DB, date int64, period string) int64 {
	retDate := time.Now()
	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)
	//CheckError(db, err, true, true, "")
	tm := time.Unix(date, 0).In(location)
	switch period {

	case "dialy":
		retDate = tm.AddDate(0, 0, -1)

	case "weekly":
		BeforeOneWeek := tm.AddDate(0, 0, -7)
		Year, Week := BeforeOneWeek.ISOWeek()
		location := BeforeOneWeek.Location()
		retDate = firstDayOfISOWeek(Year, Week, location)

	case "monthly":
		BeforeOneMonth := tm.AddDate(0, -1, 0)
		retDate, _ = monthInterval(BeforeOneMonth)
	case "yearly":

		retDate = tm.AddDate(-1, 0, 0)

	}
	return retDate.Unix()
}

func CurrentCalcDate(db *sql.DB, date int64, period string) int64 {
	retDate := time.Now()

	//tm := time.Unix(date, 0)
	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)
	//CheckError(db, err, true, true, "")
	tm := time.Unix(date, 0).In(location)

	switch period {

	case "weekly":
		BeforeOneWeek := tm
		Year, Week := BeforeOneWeek.ISOWeek()
		location := BeforeOneWeek.Location()
		retDate = firstDayOfISOWeek(Year, Week, location)

	case "monthly":
		BeforeOneMonth := tm
		retDate, _ = monthInterval(BeforeOneMonth)

	}
	return retDate.Unix()
}

func NextCalcDate(db *sql.DB, date int64, period string) int64 {
	retDate := time.Now()
	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)
	//CheckError(db, err, true, true, "")
	tm := time.Unix(date, 0).In(location)
	switch period {

	case "daily":
		retDate = tm.AddDate(0, 0, 1)
		y, m, d := retDate.Date()
		loc := tm.Location()
		retDate = time.Date(y, m, d, 0, 0, 0, 0, loc)

	case "weekly":
		BeforeOneWeek := tm.AddDate(0, 0, 7)
		Year, Week := BeforeOneWeek.ISOWeek()
		location := BeforeOneWeek.Location()
		retDate = firstDayOfISOWeek(Year, Week, location)

	case "monthly":
		BeforeOneMonth := tm.AddDate(0, 1, 0)
		retDate, _ = monthInterval(BeforeOneMonth)
	case "yearly":

		retDate = tm.AddDate(1, 0, 0)

	}
	return retDate.Unix()
}

func firstDayOfISOWeek(year int, week int, timezone *time.Location) time.Time {
	date := time.Date(year, 0, 0, 0, 0, 0, 0, timezone)
	isoYear, isoWeek := date.ISOWeek()
	for date.Weekday() != time.Monday { // iterate back to Monday
		date = date.AddDate(0, 0, -1)
		isoYear, isoWeek = date.ISOWeek()
	}
	for isoYear < year { // iterate forward to the first day of the first week
		date = date.AddDate(0, 0, 1)
		isoYear, isoWeek = date.ISOWeek()
	}
	for isoWeek < week { // iterate forward to the first day of the given week
		date = date.AddDate(0, 0, 1)
		isoYear, isoWeek = date.ISOWeek()
	}
	return date
}
func monthInterval(t time.Time) (firstDay, lastDay time.Time) {
	y, m, _ := t.Date()
	loc := t.Location()
	firstDay = time.Date(y, m, 1, 0, 0, 0, 0, loc)
	lastDay = time.Date(y, m+1, 1, 0, 0, 0, -1, loc)
	return firstDay, lastDay
}

func Commision_amount(commision string, amount string) float64 {
	if len(commision) > 0 && len(amount) > 0 {
		if strings.Contains(commision, "%") {
			percentage_Str := strings.Replace(commision, "%", "", -1)
			percentage, _ := strconv.ParseFloat(percentage_Str, 64)
			amountF, _ := strconv.ParseFloat(amount, 64)
			return amountF * percentage * 0.01

		} else {
			c, _ := strconv.ParseFloat(commision, 64)
			return c
		}
	}
	return 0
}

func Member_transaction(db *sql.DB, fields map[string]string, business bool, do_check bool, master bool, bonusType string) error {

	//check the transaction if do check already enabled
	row_count := 0
	if do_check == true {
		cond := ""
		cond += " uid = " + fields["uid"] + " AND"
		cond += " associated_user_id = " + fields["associated_user_id"] + " AND"
		cond += " category = '" + fields["category"] + "'"
		row_count, _ = GoDB.GoRowCount(db, "afl_user_transactions", cond)
	}

	if len(fields["merchant_id"]) <= 0 {
		fields["merchant_id"] = Marchant_id()
	}

	if len(fields["project_name"]) <= 0 {
		fields["project_name"] = Project_name()
	}

	if row_count <= 0 {

		balance, _ := strconv.ParseFloat(fields["amount_paid"], 64)

		if fields["credit_status"] != "1" {
			balance *= -1
		}

		balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
		fields["balance"] = balance_str

		//Insert the data in to the member transaction field
		_, err := GoDB.GoInsert(db, "afl_user_transactions", fields)
		if err != nil {
			CheckError(db, err, true, true, "")
			// PR("User trans error------------", err)
			return err
		}

		if master == true {
			uid, _ := strconv.Atoi(fields["uid"])
			bonusType = strings.ToLower(fields["category"])
			bonusType = strings.Replace(bonusType, " ", "_", -1)
			/*if(bonusType == "daily_sharing" || bonusType == "coin_sharing") {
				bonusType = "sharing"
			}*/
			// PR("balance_str =============", balance_str)
			// PR("Category =============", fields["category"])
			MasterTransactionUpdate(db, uid, "commission_balance", balance_str, false, "1")
			// MasterTransactionUpdate(db, uid, "commission_"+bonusType, fields["amount_paid"], true, "1")
		}

		var businessFields map[string]string
		businessFields = make(map[string]string, 14)
		if business == true {
			businessFields["associated_user_id"] = fields["associated_user_id"]
			businessFields["uid"] = fields["uid"]
			if fields["credit_status"] == "1" {
				businessFields["credit_status"] = "0"
			} else {
				businessFields["credit_status"] = "1"
			}
			balance *= -1
			balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
			businessFields["amount_paid"] = fields["amount_paid"]
			businessFields["currency_code"] = fields["currency_code"]
			businessFields["balance"] = balance_str
			businessFields["category"] = fields["category"]
			businessFields["notes"] = fields["notes"]
			businessFields["order_id"] = fields["order_id"]
			businessFields["transaction_day"] = fields["transaction_day"]
			businessFields["transaction_month"] = fields["transaction_month"]
			businessFields["transaction_year"] = fields["transaction_year"]
			businessFields["transaction_week"] = fields["transaction_week"]
			businessFields["transaction_date"] = fields["transaction_date"]
			businessFields["created"] = fields["created"]
			Business_transaction(db, businessFields, false)
		}

	}
	return nil

}

func Member_overall_transaction(db *sql.DB, fields map[string]string, business bool, do_check bool, master bool, bonusType string) error {

	//check the transaction if do check already enabled
	row_count := 0
	if do_check == true {
		cond := ""
		cond += " uid = " + fields["uid"] + " AND"
		cond += " associated_user_id = " + fields["associated_user_id"] + " AND"
		cond += " category = '" + fields["category"] + "'"
		row_count, _ = GoDB.GoRowCount(db, "afl_user_overall_transaction", cond)
	}

	if row_count <= 0 {

		var order_id string
		if _, ok := fields["order_id"]; ok {
			order_id = fields["order_id"]
			delete(fields, "order_id")
		}

		balance, _ := strconv.ParseFloat(fields["amount_paid"], 64)

		if fields["credit_status"] != "1" {
			balance *= -1
		}

		balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
		fields["balance"] = balance_str

		//Insert the data in to the member transaction field
		_, err := GoDB.GoInsert(db, "afl_user_overall_transaction", fields)
		if err != nil {
			CheckError(db, err, true, true, "")
			// PR("over all trans error------------", err)
			return err
		}
		//Insert or update the data in the member fund table
		var userfund map[string]string
		userfund = make(map[string]string, 4)
		userfund["uid"] = fields["uid"]
		userfund["balance"] = fields["balance"]
		userfund["currency_code"] = fields["currency_code"]
		userfund["modified"] = fields["created"]

		//check the uid already in the user fund the add update the balance
		cond := ""
		cond += " uid = " + fields["uid"]

		user_fund_count, _ := GoDB.GoRowCount(db, "afl_user_fund", cond)

		if user_fund_count > 0 {
			var m map[string]string
			GoDB.GoUpdate(db, "afl_user_fund", m, "", "balance = balance + "+balance_str)
		} else {
			GoDB.GoInsert(db, "afl_user_fund", userfund)
		}

		if master == true {
			uid, _ := strconv.Atoi(fields["uid"])
			bonusType = strings.ToLower(fields["category"])
			bonusType = strings.Replace(bonusType, " ", "_", -1)
			MasterTransactionUpdate(db, uid, "overall_"+bonusType, balance_str, true, "6")
		}

		var businessFields map[string]string
		businessFields = make(map[string]string, 14)
		if business == true {
			businessFields["associated_user_id"] = fields["associated_user_id"]
			businessFields["uid"] = fields["uid"]
			if fields["credit_status"] == "1" {
				businessFields["credit_status"] = "0"
			} else {
				businessFields["credit_status"] = "1"
			}
			if len(fields["calc_details"]) > 0 {
				businessFields["calc_details"] = fields["calc_details"]
			}
			balance *= -1
			balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
			businessFields["amount_paid"] = fields["amount_paid"]
			businessFields["currency_code"] = fields["currency_code"]
			businessFields["balance"] = balance_str
			businessFields["category"] = fields["category"]
			businessFields["notes"] = fields["notes"]
			businessFields["order_id"] = order_id
			businessFields["transaction_day"] = fields["transaction_day"]
			businessFields["transaction_month"] = fields["transaction_month"]
			businessFields["transaction_year"] = fields["transaction_year"]
			businessFields["transaction_week"] = fields["transaction_week"]
			businessFields["transaction_date"] = fields["transaction_date"]
			businessFields["created"] = fields["created"]
			// PR("Business-trans -------------------------- ", businessFields)
			Business_transaction(db, businessFields, false)
		}

	}
	return nil

}

func Business_transaction(db *sql.DB, fields map[string]string, do_check bool) {

	if len(fields["created"]) < 0 {
		afl_date := Epsdate(db)
		afl_date_split := Go_date_splits(db, afl_date)
		afl_date_I := int(afl_date)
		afl_date_str := strconv.Itoa(afl_date_I)

		d := strconv.Itoa(afl_date_split["d"])
		m := strconv.Itoa(afl_date_split["m"])
		y := strconv.Itoa(afl_date_split["y"])
		w := strconv.Itoa(afl_date_split["w"])
		combine_d := y + "-" + m + "-" + d

		fields["created"] = afl_date_str
		fields["transaction_day"] = d
		fields["transaction_month"] = m
		fields["transaction_year"] = y
		fields["transaction_week"] = w
		fields["transaction_date"] = combine_d

		balance, _ := strconv.ParseFloat(fields["amount_paid"], 64)

		if fields["credit_status"] != "1" {
			balance *= -1
		}

		balance_str := strconv.FormatFloat(balance, 'f', 3, 64)

		fields["balance"] = balance_str
	}

	if len(fields["merchant_id"]) <= 0 {
		fields["merchant_id"] = Marchant_id()
	}

	if len(fields["project_name"]) <= 0 {
		fields["project_name"] = Project_name()
	}

	row_count := 0
	if do_check == true {
		cond := ""
		cond += " uid = " + fields["uid"] + " AND"
		cond += " associated_user_id = " + fields["associated_user_id"] + " AND"
		cond += " category = '" + fields["category"] + "'"
		row_count, _ = GoDB.GoRowCount(db, "afl_business_transactions", cond)
	}

	if row_count <= 0 {
		_, err := GoDB.GoInsert(db, "afl_business_transactions", fields)
		if err != nil {
			CheckError(db, err, true, true, "")
			// PR("Business trans error------------", err)
		}
	}

}

func ConvertBtc(db *sql.DB, amount string) string {
	//convert to float
	amount_float, _ := strconv.ParseFloat(amount, 64)
	//get the ratio from database
	ratio_str, _ := GoDB.AFLVariableGet(db, "afl_compensations_bitcoin_vs_doller")
	ratio, _ := strconv.ParseFloat(ratio_str, 64)

	amount_float *= ratio
	btc := strconv.FormatFloat(amount_float, 'f', 8, 64)
	//convert in to btc
	return btc
}

func Token_transaction(db *sql.DB, fields map[string]string, business bool, do_check bool) {

	//check the transaction if do check already enabled
	row_count := 0
	if do_check == true {
		cond := ""
		cond += " uid = " + fields["uid"] + " AND"
		cond += " associated_user_id = " + fields["associated_user_id"] + " AND"
		cond += " category = '" + fields["category"] + "'"
		row_count, _ = GoDB.GoRowCount(db, "afl_user_token_transactions", cond)
	}

	/*if len(fields["merchant_id"]) <= 0 {
	    fields["merchant_id"] = Marchant_id()
	  }

	  if len(fields["project_name"]) <= 0 {
	    fields["project_name"] = Project_name()
	  }*/

	if row_count <= 0 {

		balance, _ := strconv.ParseFloat(fields["amount_paid"], 64)

		if fields["credit_status"] != "1" {
			balance *= -1
		}

		balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
		fields["balance"] = balance_str

		//Insert the data in to the member transaction field
		GoDB.GoInsert(db, "afl_user_token_transactions", fields)
		//Insert or update the data in the member fund table

		var businessFields map[string]string
		businessFields = make(map[string]string, 14)
		if business == true {
			businessFields["associated_user_id"] = fields["associated_user_id"]
			businessFields["uid"] = fields["uid"]
			if fields["credit_status"] == "1" {
				businessFields["credit_status"] = "0"
			} else {
				businessFields["credit_status"] = "1"
			}
			balance *= -1
			balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
			businessFields["amount_paid"] = fields["amount_paid"]
			businessFields["currency_code"] = fields["currency_code"]
			businessFields["balance"] = balance_str
			businessFields["category"] = fields["category"]
			businessFields["notes"] = fields["notes"]
			businessFields["order_id"] = fields["order_id"]
			businessFields["transaction_day"] = fields["transaction_day"]
			businessFields["transaction_month"] = fields["transaction_month"]
			businessFields["transaction_year"] = fields["transaction_year"]
			businessFields["transaction_week"] = fields["transaction_week"]
			businessFields["transaction_date"] = fields["transaction_date"]
			businessFields["created"] = fields["created"]

			Business_token_transaction(db, businessFields, false)
		}

	}

}

func Business_token_transaction(db *sql.DB, fields map[string]string, do_check bool) {

	if len(fields["created"]) < 0 {
		afl_date := Epsdate(db)
		afl_date_split := Go_date_splits(db, afl_date)
		afl_date_I := int(afl_date)
		afl_date_str := strconv.Itoa(afl_date_I)

		d := strconv.Itoa(afl_date_split["d"])
		m := strconv.Itoa(afl_date_split["m"])
		y := strconv.Itoa(afl_date_split["y"])
		w := strconv.Itoa(afl_date_split["w"])
		combine_d := y + "-" + m + "-" + d

		fields["created"] = afl_date_str
		fields["transaction_day"] = d
		fields["transaction_month"] = m
		fields["transaction_year"] = y
		fields["transaction_week"] = w
		fields["transaction_date"] = combine_d

	}

	/*if len(fields["merchant_id"]) <= 0 {
	    fields["merchant_id"] = Marchant_id()
	  }

	  if len(fields["project_name"]) <= 0 {
	    fields["project_name"] = Project_name()
	  }*/

	row_count := 0
	if do_check == true {
		cond := ""
		cond += " uid = " + fields["uid"] + " AND"
		cond += " associated_user_id = " + fields["associated_user_id"] + " AND"
		cond += " category = '" + fields["category"] + "'"
		row_count, _ = GoDB.GoRowCount(db, "afl_business_transactions", cond)
	}

	if row_count <= 0 {
		GoDB.GoInsert(db, "afl_business_token_transaction", fields)
	}

}

func Bitcoin_transaction(db *sql.DB, fields map[string]string, business bool, do_check bool) {

	//check the transaction if do check already enabled
	row_count := 0
	if do_check == true {
		cond := ""
		cond += " uid = " + fields["uid"] + " AND"
		cond += " associated_user_id = " + fields["associated_user_id"] + " AND"
		cond += " category = '" + fields["category"] + "'"
		row_count, _ = GoDB.GoRowCount(db, "afl_user_token_transactions", cond)
	}

	/*if len(fields["merchant_id"]) <= 0 {
	    fields["merchant_id"] = Marchant_id()
	  }

	  if len(fields["project_name"]) <= 0 {
	    fields["project_name"] = Project_name()
	  }*/

	if row_count <= 0 {

		balance, _ := strconv.ParseFloat(fields["amount_paid"], 64)

		if fields["credit_status"] != "1" {
			balance *= -1
		}

		balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
		fields["balance"] = balance_str

		//Insert the data in to the member transaction field
		GoDB.GoInsert(db, "afl_bitcoin_user_transactions", fields)
		//Insert or update the data in the member fund table

		var businessFields map[string]string
		businessFields = make(map[string]string, 14)
		if business == true {
			businessFields["associated_user_id"] = fields["associated_user_id"]
			businessFields["uid"] = fields["uid"]
			if fields["credit_status"] == "1" {
				businessFields["credit_status"] = "0"
			} else {
				businessFields["credit_status"] = "1"
			}
			balance *= -1
			balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
			businessFields["amount_paid"] = fields["amount_paid"]
			businessFields["currency_code"] = fields["currency_code"]
			businessFields["balance"] = balance_str
			businessFields["category"] = fields["category"]
			businessFields["notes"] = fields["notes"]
			businessFields["order_id"] = fields["order_id"]
			businessFields["transaction_day"] = fields["transaction_day"]
			businessFields["transaction_month"] = fields["transaction_month"]
			businessFields["transaction_year"] = fields["transaction_year"]
			businessFields["transaction_week"] = fields["transaction_week"]
			businessFields["transaction_date"] = fields["transaction_date"]
			businessFields["created"] = fields["created"]

			Business_bitcoin_transaction(db, businessFields, false)
		}

	}

}

func Business_bitcoin_transaction(db *sql.DB, fields map[string]string, do_check bool) {

	if len(fields["created"]) < 0 {
		afl_date := Epsdate(db)
		afl_date_split := Go_date_splits(db, afl_date)
		afl_date_I := int(afl_date)
		afl_date_str := strconv.Itoa(afl_date_I)

		d := strconv.Itoa(afl_date_split["d"])
		m := strconv.Itoa(afl_date_split["m"])
		y := strconv.Itoa(afl_date_split["y"])
		w := strconv.Itoa(afl_date_split["w"])
		combine_d := y + "-" + m + "-" + d

		fields["created"] = afl_date_str
		fields["transaction_day"] = d
		fields["transaction_month"] = m
		fields["transaction_year"] = y
		fields["transaction_week"] = w
		fields["transaction_date"] = combine_d

	}

	/*if len(fields["merchant_id"]) <= 0 {
	    fields["merchant_id"] = Marchant_id()
	  }

	  if len(fields["project_name"]) <= 0 {
	    fields["project_name"] = Project_name()
	  }*/

	row_count := 0
	if do_check == true {
		cond := ""
		cond += " uid = " + fields["uid"] + " AND"
		cond += " associated_user_id = " + fields["associated_user_id"] + " AND"
		cond += " category = '" + fields["category"] + "'"
		row_count, _ = GoDB.GoRowCount(db, "afl_business_transactions", cond)
	}

	if row_count <= 0 {
		GoDB.GoInsert(db, "afl_bitcoin_business_transactions", fields)
	}

}

func Marchant_id() string {
	return "default"
}

func Project_name() string {
	return "default"
}

/*
 * ----------------------------------------------------------------------------------
 * Returns the currency code of the site's default currency.
 * ----------------------------------------------------------------------------------
 */
func CommerceDeafaultCurrency(db *sql.DB) string {
	currency_code, _ := GoDB.VariableGet(db, "commerce_default_currency")
	if len(currency_code) > 0 {
		return currency_code
	} else {
		return "USD"
	}

}

/*
 * ----------------------------------------------------------------------------------
 * Get user Current package ID
 * ----------------------------------------------------------------------------------
 */
func UserCurrentPackageId(db *sql.DB, uid int) string {
	/*table := "afl_user_genealogy"
	  cond := " uid = " + strconv.Itoa(uid)
	  pkg_id, _ := GoDB.FetchField(db, table, "enrolment_package_id", cond)
	  return pkg_id*/
	table := "afl_user_sales_volume"
	cond := " uid = " + strconv.Itoa(uid)
	pkg_id, _ := GoDB.FetchField(db, table, "package_id", cond)
	// PR(pkg_id)
	return pkg_id
}

/*
 * ----------------------------------------------------------------------------------
 * Get user Current package ID
 * ----------------------------------------------------------------------------------
 */
func UserCurrentPackageIdV1(db *sql.DB, uid int) string {
	/*table := "afl_user_genealogy"
	  cond := " uid = " + strconv.Itoa(uid)
	  pkg_id, _ := GoDB.FetchField(db, table, "enrolment_package_id", cond)
	  return pkg_id*/
	var pkg_id string
	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)
	date := Epsdate(db)
	time_period := time.Unix(date, 0).In(location).Format("2006-01-02 00:00:00")
	timeStrtoTime := Strtotime(db, time_period)
	time_periodI := int(timeStrtoTime)

	table := "afl_user_sales_volume"
	cond := " uid = " + strconv.Itoa(uid)
	data, _ := GoDB.FetchAssoc(db, table, "package_id, last_package_id, package_up_on", "", cond)
	pkg_up := data["package_up_on"]
	pkg_upI, _ := strconv.Atoi(pkg_up)
	PR(data)
	if pkg_upI > time_periodI {
		pkg_id = data["last_package_id"]
	} else {
		pkg_id = data["package_id"]
	}
	// PR(pkg_id)
	return pkg_id
}

/*
 * ----------------------------------------------------------------------------------
 * Get product compensation attributes from table
 * ----------------------------------------------------------------------------------
 */
func GetProductCompensationAttributes(db *sql.DB, product_id string, fields string) map[int]map[string]string {

	if len(fields) < 0 {
		fields = " * "
	}
	table := "afl_product_compensation_attributes"
	var cond string
	if len(product_id) > 0 {
		cond = "product_id = " + product_id
	} else {
		fields += ",product_id"

	}

	data, _ := GoDB.FetchAll(db, table, fields, "", cond, "", "")

	return data
}

/**
 * ----------------------------------------------------
 *  FUNC : Get_master_data
 *  This function used to get the master table sales
 *  @param uids array of uids
 *  @return master table details
 *  ---------------------------------------------------
 */

func Get_master_data(db *sql.DB, tb string, uids []string, custom_cond string) map[string]map[string]map[string]string {
	var m map[string]map[string]map[string]string
	m = make(map[string]map[string]map[string]string, 0)
	//Get the master data
	cond := ""
	if len(uids) > 0 {
		UidChunk := ChunkArray(uids, 50)
		Or := " ("
		ChunkCount := len(UidChunk)
		var i int
		for _, val := range UidChunk {
			if i++; i < ChunkCount {
				Or += "uid IN (" + strings.Join(val, ",") + ") OR "
			} else {
				Or += "uid IN (" + strings.Join(val, ",") + ")"
			}
		}
		Or += " ) "
		cond = Or
	}

	if len(custom_cond) > 0 {
		cond += " AND " + custom_cond
	}

	fields := `uid,
                 particulars,
                 category,
                 SUM(left_vol) as total_left_vol,
                 SUM(right_vol) as total_right_vol,
                 SUM(total_vol) as total_sum_vol`

	group_cond := ""
	group_cond += "uid,particulars,category"
	data, _ := GoDB.FetchAllOrderGroup(db, tb, fields, "", cond, "", "", "", group_cond)

	if len(data) > 0 {
		for _, val := range data {
			if len(m[val["uid"]]) > 0 {
				m[val["uid"]][val["particulars"]] = val
			} else {
				m[val["uid"]] = map[string]map[string]string{
					val["particulars"]: val,
				}
			}

		}
	}

	return m
}

func In_array(val string, array []string) (exists bool) {
	exists = false
	for _, v := range array {
		if val == v {
			exists = true
			return
		}
	}

	return
}
func In_array_float(val float64, array []float64) (exists bool) {
	exists = false
	for _, v := range array {
		if val == v {
			exists = true
			return
		}
	}

	return
}

func ProductAmount(db *sql.DB, product_id string) (val string) {

	if product_id != "" {
		val, _ = GoDB.FetchField(db, "field_data_commerce_price", "commerce_price_amount", "entity_id = "+product_id)
	}
	return val
}

func Get_active_uids(db *sql.DB, uids map[string][]string) map[string][]string {
	var activeUids map[string][]string
	activeUids = make(map[string][]string, 0)
	cond := " left_bonus_eligible_from <> 0 AND right_bonus_eligible_from <> 0 AND personal_bonus_eligible = 1"
	Orl := ""
	Or := ""
	if len(uids["LEFT"]) > 0 {
		UidChunkl := ChunkArray(uids["LEFT"], 50)
		Orl += " ("
		ChunkCountl := len(UidChunkl)
		var i int
		for _, vall := range UidChunkl {
			if i++; i < ChunkCountl {
				Orl += "uid IN (" + strings.Join(vall, ",") + ") OR "
			} else {
				Orl += "uid IN (" + strings.Join(vall, ",") + ")"
			}

		}
		Orl += " ) "

	}

	if len(uids["RIGHT"]) > 0 {
		UidChunkr := ChunkArray(uids["RIGHT"], 50)
		Or += " ("
		ChunkCountr := len(UidChunkr)
		var j int
		for _, valr := range UidChunkr {
			if j++; j < ChunkCountr {
				Or += "uid IN (" + strings.Join(valr, ",") + ") OR "
			} else {
				Or += "uid IN (" + strings.Join(valr, ",") + ")"
			}

		}
		Or += " ) "

	}

	if Orl != "" && Or != "" {
		cond += " AND ( " + Orl + " OR " + Or + " )"
	} else if Orl != "" {
		cond += " AND " + Orl
	} else if Or != "" {
		cond += " AND " + Or
	}

	if cond != "" {
		a_uids, _ := GoDB.FetchCol(db, "afl_user_extra", "uid", "", cond)

		if len(a_uids) > 0 {
			for _, uid_e := range a_uids {
				if In_array(uid_e, uids["LEFT"]) {
					if len(activeUids["LEFT"]) > 0 {
						activeUids["LEFT"] = append(activeUids["LEFT"], uid_e)
					} else {
						activeUids["LEFT"] = []string{
							uid_e,
						}
					}

				} else if In_array(uid_e, uids["RIGHT"]) {
					if len(activeUids["RIGHT"]) > 0 {
						activeUids["RIGHT"] = append(activeUids["RIGHT"], uid_e)

					} else {
						activeUids["RIGHT"] = []string{
							uid_e,
						}
					}

				}
			}
		}
	}

	return activeUids
}

func Get_active_uids_cond(db *sql.DB, uids map[string][]string, condS string) map[string][]string {
	var activeUids map[string][]string
	activeUids = make(map[string][]string, 0)
	cond := " left_bonus_eligible_from <> 0 AND right_bonus_eligible_from <> 0 AND personal_bonus_eligible = 1"
	if len(condS) > 0 {
		cond += " AND " + condS
	}
	Orl := ""
	Or := ""
	if len(uids["LEFT"]) > 0 {
		UidChunkl := ChunkArray(uids["LEFT"], 50)
		Orl += " ("
		ChunkCountl := len(UidChunkl)
		var i int
		for _, vall := range UidChunkl {
			if i++; i < ChunkCountl {
				Orl += "uid IN (" + strings.Join(vall, ",") + ") OR "
			} else {
				Orl += "uid IN (" + strings.Join(vall, ",") + ")"
			}

		}
		Orl += " ) "

	}

	if len(uids["RIGHT"]) > 0 {
		UidChunkr := ChunkArray(uids["RIGHT"], 50)
		Or += " ("
		ChunkCountr := len(UidChunkr)
		var j int
		for _, valr := range UidChunkr {
			if j++; j < ChunkCountr {
				Or += "uid IN (" + strings.Join(valr, ",") + ") OR "
			} else {
				Or += "uid IN (" + strings.Join(valr, ",") + ")"
			}

		}
		Or += " ) "

	}

	if Orl != "" && Or != "" {
		cond += " AND ( " + Orl + " OR " + Or + " )"
	} else if Orl != "" {
		cond += " AND " + Orl
	} else if Or != "" {
		cond += " AND " + Or
	}

	if cond != "" {
		a_uids, _ := GoDB.FetchCol(db, "afl_user_extra", "uid", "", cond)

		if len(a_uids) > 0 {
			for _, uid_e := range a_uids {
				if In_array(uid_e, uids["LEFT"]) {
					if len(activeUids["LEFT"]) > 0 {
						activeUids["LEFT"] = append(activeUids["LEFT"], uid_e)
					} else {
						activeUids["LEFT"] = []string{
							uid_e,
						}
					}

				} else if In_array(uid_e, uids["RIGHT"]) {
					if len(activeUids["RIGHT"]) > 0 {
						activeUids["RIGHT"] = append(activeUids["RIGHT"], uid_e)

					} else {
						activeUids["RIGHT"] = []string{
							uid_e,
						}
					}

				}
			}
		}
	}

	return activeUids
}

func RemoveRole(db *sql.DB, uid int, role string) bool {
	uid_Str := strconv.Itoa(uid)
	rid, err := GoDB.FetchField(db, "role", "rid", " name LIKE '"+role+"'")

	if err != nil {
		return false
	}
	rid_bool := StringStatusCheck(rid)
	if rid_bool == false {
		return false
	}

	status, _ := GoDB.GoDelete(db, "users_roles", "uid = "+uid_Str+" AND rid = "+rid)

	return status
}

func AddRole(db *sql.DB, uid int, role string) bool {
	uid_Str := strconv.Itoa(uid)
	rid, err := GoDB.FetchField(db, "role", "rid", " name LIKE '"+role+"'")

	if err != nil {
		return false
	}
	rid_bool := StringStatusCheck(rid)
	if rid_bool == false {
		return false
	}

	exist, _ := GoDB.GoRowCount(db, "users_roles", "uid = "+uid_Str+" AND rid = "+rid)
	if exist == 0 {
		fields := make(map[string]string, 2)
		fields["uid"] = uid_Str
		fields["rid"] = rid
		GoDB.GoInsert(db, "users_roles", fields)
	}

	return true
}

func CheckRole(db *sql.DB, uid int, role string) int {
	uid_Str := strconv.Itoa(uid)
	rid, err := GoDB.FetchField(db, "role", "rid", " name LIKE '"+role+"'")

	if err != nil {
		return 0
	}
	rid_bool := StringStatusCheck(rid)
	if rid_bool == false {
		return 0
	}

	exist, _ := GoDB.GoRowCount(db, "users_roles", "uid = "+uid_Str+" AND rid = "+rid)

	return exist
}

func UserPurchaseAmount(db *sql.DB, uid int) map[int]map[string]string {
	uid_Str := strconv.Itoa(uid)
	cond := " category IN ('pay-later','upgrade-package') AND uid = " + uid_Str
	purchase_amount, _ := GoDB.FetchAll(db, "afl_purchases", "amount_paid,purchase_week,purchase_month,purchase_day", "", cond, "", "")

	return purchase_amount

}

func MaintanceMode(db *sql.DB, tb string, cond string) (bool, string) {
	Mode, _ := GoDB.VariableGet(db, "maintenance_mode")
	CronStop, _ := GoDB.VariableGet(db, "stop_cron")
	var log string
	if tb != "" {
		count, _ := GoDB.GoRowCount(db, tb, cond)
		log = PS("%v number of entities need to completed in %v", count, tb)
		// PR(log)

	}
	return StringStatusCheck(Mode) || StringStatusCheck(CronStop), log
}

func Reverse(ss []string) []string {
	last := len(ss) - 1
	for i := 0; i < len(ss)/2; i++ {
		ss[i], ss[last-i] = ss[last-i], ss[i]
	}

	return ss
}

/**
* SolidusPointCalculater converted BV ito Doller and vice-versa
 */
func SolidusPointCalculater(db *sql.DB, value string, usd bool) string {
	/*usd_to_bv, _ := GoDB.AFLVariableGet(db, "solidus_plan_dollar_to_bv_ratio")
	  ratio, err := strconv.ParseFloat(usd_to_bv, 64)
	  FlotValue, err1 := strconv.ParseFloat(value, 64)

	  var result float64 = 0
	  if err != nil || err1 != nil {
	    return result
	  }

	  if usd {
	    result = FlotValue * (ratio / 100)
	  } else {
	    result = (FlotValue * 100) / ratio
	  }
	  return result*/

	usd_to_bv, _ := GoDB.AFLVariableGet(db, "solidus_plan_dollar_to_bv_ratio")

	v, _ := strconv.ParseFloat(value, 64)
	v1, _ := strconv.ParseFloat(usd_to_bv, 64)
	v1_str := strconv.FormatFloat(v1, 'f', 3, 64)
	v1_status := StringStatusCheck(v1_str)

	s := float64(0)

	if v1_status {
		if usd {
			s = v1 / 100
		} else {
			s = v * v1
		}

	}
	s *= v

	amount := strconv.FormatFloat(s, 'f', 3, 64)

	return amount
}

/**
* Custom cron calulation date
* This function returns cron calcution date depends on PERIOD
* Weekly - Returns Date Of Previous Sunday With Time 00Hr:01Min of the Given Date
* Monthly - Returns Date Of Month starting [1st of Given month] With Time 00Hr:01Min of the Given Date
* Yearly - Returns Date Of Year starting [Jan 1st of Given Year] With Time 00Hr:01Min of the Given Date
 */

func CronCalculationDate(dateUnix int64, period string) int64 {
	date := time.Unix(dateUnix, 0)
	location := date.Location()
	switch period {
	case "weekly":
		for date.Weekday() != time.Sunday { // iterate back to Sunday
			date = date.AddDate(0, 0, -1)
		}
		year, month, day := date.Date()
		date = time.Date(year, month, day, 04, 31, 0, 0, location)
	case "monthly":
		year, month, _ := date.Date()
		date = time.Date(year, month, 1, 04, 31, 0, 0, location)

	case "yearly":
		year, _, _ := date.Date()
		date = time.Date(year, 1, 1, 05, 31, 0, 0, location)
	}
	return date.Unix()
}

func ModuleExists(moduleName string) bool {
	db, err := GoDB.Getdbc()
	defer db.Close()
	if err != nil {
		panic(err)
	}

	count, _ := GoDB.GoRowCount(db, "system", "type = 'module' AND name = '"+moduleName+"' AND status = 1")

	if count > 0 {
		return true
	} else {
		return false
	}
}

func CommerceProductLoadByLineitemId(db *sql.DB, line_item_id string, join string, extra_cond string) (product map[string]string, err error) {

	sqlSmt := "LEFT JOIN commerce_line_item ON commerce_line_item.line_item_label = commerce_product.sku" + " LEFT JOIN field_data_commerce_price ON commerce_product.product_id = field_data_commerce_price.entity_id"
	cond := "commerce_line_item.line_item_id=" + line_item_id
	if len(join) > 0 {
		sqlSmt = sqlSmt + " " + join
	}
	if len(extra_cond) > 0 {
		cond = cond + " AND " + extra_cond
	}
	product, err = GoDB.FetchAssoc(db, "commerce_product",
		"*",
		// "commerce_product.product_id,commerce_product.revision_id,commerce_product.sku,commerce_product.title,commerce_product.type,commerce_line_item.line_item_id,commerce_line_item.order_id,commerce_line_item.type,commerce_line_item.line_item_label,commerce_line_item.quantity",
		sqlSmt, cond)

	return
}

/*
 * -----------------------------------------------------------------------
 * Split the given timestamp
 * -----------------------------------------------------------------------
 */
func Go_get_weekday(db *sql.DB, timestamp int64, str bool) string {

	days := make(map[int]string)
	days[0] = "Sunday"
	days[1] = "Monday"
	days[2] = "Tuesday"
	days[3] = "Wednesday"
	days[4] = "Thursday"
	days[5] = "Friday"
	days[6] = "Saturday"
	delta := EpsdateDev(db) - Epsdate(db)
	timestamp += delta

	currentTime := time.Unix(timestamp, 0)

	weekDay := currentTime.Weekday()

	// Declare typed constants each with type of Weekday
	day := int(weekDay)

	if str == true {
		return days[day]
	} else {
		Weekday := strconv.Itoa(day)
		return Weekday
	}

}

func UserCurrentSV(db *sql.DB, Uid int) (SV string, err error) {
	uidStr := strconv.Itoa(Uid)
	SV, err = GoDB.FetchField(db, "afl_user_sales_volume", "sv", "uid = "+uidStr)
	return
}
func UserCurrentSVDS(db *sql.DB, Uid int) (SV string) {
	// var pkg_id string
	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)
	date := Epsdate(db)
	time_period := time.Unix(date, 0).In(location).Format("2006-01-02 00:00:00")
	timeStrtoTime := Strtotime(db, time_period)
	time_periodI := int(timeStrtoTime)

	table := "afl_user_sales_volume"
	cond := " uid = " + strconv.Itoa(Uid)
	data, _ := GoDB.FetchAssoc(db, table, "daily_share_rate, last_sv, sv_up_on", "", cond)
	pkg_up := data["sv_up_on"]
	pkg_upI, _ := strconv.Atoi(pkg_up)
	if pkg_upI > time_periodI {
		SV = data["last_sv"]
	} else {
		SV = data["daily_share_rate"]
	}
	return
}

func Go_get_next_weekday(db *sql.DB, Weekday string) (startdate int64) {
	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)

	date := Epsdate(db)
	currentTime := time.Unix(date, 0)
	week := currentTime.Weekday()
	day := int(week)
	days := make(map[int]string)
	days[0] = "Sunday"
	days[1] = "Monday"
	days[2] = "Tuesday"
	days[3] = "Wednesday"
	days[4] = "Thursday"
	days[5] = "Friday"
	days[6] = "Saturday"

	var end, add int
	for k, val := range days {
		if val == Weekday {
			end = k
		}
	}
	next_day := end - day
	if next_day <= 0 {
		length := len(days)
		add = next_day + length
	} else {
		add = next_day
	}

	// startdate = StrtotimeDev(time.Unix(int64(date), 0).AddDate(0, 0, add).In(location).Format("2006-01-02")+" 00:00:00")
	startdateI := time.Unix(date, 0).AddDate(0, 0, add).In(location).Format("2006-01-02 00:00:00")
	startdate = Strtotime(db, startdateI)
	return
}

func AFLCommerceAmount(amountpaid float64) float64 {
	return amountpaid * 100.0
}

/*
*
* Multi wallet splits
* uid : user id
* category : category of walletsplit its one of
* 1. daily_sharing
* 2. coin_sharing
* 3. bonus
* 4. lucky_event
* 5. admin_recognizion
*
* payoutCategory : category of payment
 */
func MultiWalletSplit(db *sql.DB, uid int, amount string, category string, payoutCategory string, date int64, date_splits map[string]int, associated_user_id string, common_id string, notes string) (bool, error) {

	var rate string
	var Insert_fields map[int]map[string]string
	Insert_fields = make(map[int]map[string]string)

	var trans_array map[string]map[int]map[string]string //All Transaction datas
	trans_array = make(map[string]map[int]map[string]string)

	var Insert_fields_w1 map[int]map[string]string
	Insert_fields_w1 = make(map[int]map[string]string)

	var Insert_fields_w2 map[int]map[string]string
	Insert_fields_w2 = make(map[int]map[string]string)

	var Insert_fields_w3 map[int]map[string]string
	Insert_fields_w3 = make(map[int]map[string]string)

	var Insert_fields_w4 map[int]map[string]string
	Insert_fields_w4 = make(map[int]map[string]string)

	var Insert_fields_w5 map[int]map[string]string
	Insert_fields_w5 = make(map[int]map[string]string)
	/*var master_cat_fields map[string]map[int]map[string]string
	  master_cat_fields = make(map[string]map[int]map[string]string)*/
	dateI := int(date)
	dateStr := strconv.Itoa(dateI)
	d := strconv.Itoa(date_splits["d"])
	m := strconv.Itoa(date_splits["m"])
	y := strconv.Itoa(date_splits["y"])
	w := strconv.Itoa(date_splits["w"])
	dateCom := y + "-" + m + "-" + d
	uidStr := strconv.Itoa(uid)
	currency := CommerceDeafaultCurrency(db)
	wallets, _ := GoDB.AFLVariableGet(db, "afl_max_wallet")
	walletsStr, _ := strconv.Atoi(wallets)
	flag := 0
	exists, _ := GoDB.GoRowCount(db, "afl_admin_recognition", "uid = "+uidStr+" AND request_status = 'Approved'")
	if exists <= 0 {
		flag = 1
	}
	count := 0
	count_w1 := 0
	count_w2 := 0
	count_w3 := 0
	count_w4 := 0
	count_w5 := 0
	UID_array := []string{}
	for i := 1; i <= walletsStr; i++ {
		iStr := strconv.Itoa(i)
		/*var master_fields map[int]map[string]string
		master_fields = make(map[int]map[string]string)*/
		if flag == 1 {
			rate, _ = GoDB.AFLVariableGet(db, "afl_wallet_allocation_"+category+"_"+iStr)
		} else {
			rate, _ = GoDB.AFLVariableGet(db, "afl_wallet_allocation_admin_recognizion_"+iStr)
		}
		// rate,_ := GoDB.AFLVariableGet(db, "afl_wallet_allocation_"+category+"_"+iStr)
		// wallet_name,_ := GoDB.AFLVariableGet(db, "afl_wallet_name_"+iStr)

		amount_payable := Commision_amount(rate, amount)
		commision_str := strconv.FormatFloat(amount_payable, 'f', 6, 64)
		amount_strF, _ := strconv.ParseFloat(commision_str, 64)
		if amount_strF > 0.0 {
			// amount_payable = AFLCommerceAmount(amount_payable)
			// var fields map[string]string

			//Split Multi_wallet table
			switch iStr {

			case "1":
				fields_w1 := map[string]string{}
				fields_w1["uid"] = uidStr
				fields_w1["balance"] = commision_str
				fields_w1["payout_category"] = payoutCategory
				fields_w1["created"] = dateStr
				fields_w1["updated"] = dateStr
				fields_w1["wallet_category"] = iStr
				fields_w1["category"] = category
				fields_w1["transaction_day"] = d
				fields_w1["transaction_month"] = m
				fields_w1["transaction_year"] = y
				fields_w1["transaction_week"] = w
				fields_w1["transaction_date"] = dateCom
				fields_w1["credit_status"] = "1"
				fields_w1["amount_paid"] = commision_str
				fields_w1["associated_user_id"] = associated_user_id
				fields_w1["extra_params"] = dateStr
				fields_w1["common_id"] = common_id
				fields_w1["notes"] = notes
				fields_w1["currency_code"] = currency
				Insert_fields_w1[count_w1] = fields_w1
				trans_array[iStr] = Insert_fields_w1
				count_w1++

			case "2":
				fields_w2 := map[string]string{}
				fields_w2["uid"] = uidStr
				fields_w2["balance"] = commision_str
				fields_w2["payout_category"] = payoutCategory
				fields_w2["created"] = dateStr
				fields_w2["updated"] = dateStr
				fields_w2["wallet_category"] = iStr
				fields_w2["category"] = category
				fields_w2["transaction_day"] = d
				fields_w2["transaction_month"] = m
				fields_w2["transaction_year"] = y
				fields_w2["transaction_week"] = w
				fields_w2["transaction_date"] = dateCom
				fields_w2["credit_status"] = "1"
				fields_w2["amount_paid"] = commision_str
				fields_w2["associated_user_id"] = associated_user_id
				fields_w2["extra_params"] = dateStr
				fields_w2["common_id"] = common_id
				fields_w2["notes"] = notes
				fields_w2["currency_code"] = currency
				Insert_fields_w2[count_w2] = fields_w2
				trans_array[iStr] = Insert_fields_w2
				count_w2++

			case "3":
				UID_array = append(UID_array, uidStr)
				fields_w3 := map[string]string{}
				fields_w3["uid"] = uidStr
				fields_w3["balance"] = commision_str
				fields_w3["payout_category"] = payoutCategory
				fields_w3["created"] = dateStr
				fields_w3["updated"] = dateStr
				fields_w3["wallet_category"] = iStr
				fields_w3["category"] = category
				fields_w3["transaction_day"] = d
				fields_w3["transaction_month"] = m
				fields_w3["transaction_year"] = y
				fields_w3["transaction_week"] = w
				fields_w3["transaction_date"] = dateCom
				fields_w3["credit_status"] = "1"
				fields_w3["amount_paid"] = commision_str
				fields_w3["associated_user_id"] = associated_user_id
				fields_w3["extra_params"] = dateStr
				fields_w3["common_id"] = common_id
				fields_w3["notes"] = notes
				fields_w3["currency_code"] = currency
				Insert_fields_w3[count_w3] = fields_w3
				trans_array[iStr] = Insert_fields_w3
				count_w3++

			case "4":
				fields_w4 := map[string]string{}
				fields_w4["uid"] = uidStr
				fields_w4["balance"] = commision_str
				fields_w4["payout_category"] = payoutCategory
				fields_w4["created"] = dateStr
				fields_w4["updated"] = dateStr
				fields_w4["wallet_category"] = iStr
				fields_w4["category"] = category
				fields_w4["transaction_day"] = d
				fields_w4["transaction_month"] = m
				fields_w4["transaction_year"] = y
				fields_w4["transaction_week"] = w
				fields_w4["transaction_date"] = dateCom
				fields_w4["credit_status"] = "1"
				fields_w4["amount_paid"] = commision_str
				fields_w4["associated_user_id"] = associated_user_id
				fields_w4["extra_params"] = dateStr
				fields_w4["common_id"] = common_id
				fields_w4["notes"] = notes
				fields_w4["currency_code"] = currency
				Insert_fields_w4[count_w4] = fields_w4
				trans_array[iStr] = Insert_fields_w4
				count_w4++

			case "5":
				fields_w5 := map[string]string{}
				fields_w5["uid"] = uidStr
				fields_w5["balance"] = commision_str
				fields_w5["payout_category"] = payoutCategory
				fields_w5["created"] = dateStr
				fields_w5["updated"] = dateStr
				fields_w5["wallet_category"] = iStr
				fields_w5["category"] = category
				fields_w5["transaction_day"] = d
				fields_w5["transaction_month"] = m
				fields_w5["transaction_year"] = y
				fields_w5["transaction_week"] = w
				fields_w5["transaction_date"] = dateCom
				fields_w5["credit_status"] = "1"
				fields_w5["amount_paid"] = commision_str
				fields_w5["associated_user_id"] = associated_user_id
				fields_w5["extra_params"] = dateStr
				fields_w5["common_id"] = common_id
				fields_w5["notes"] = notes
				fields_w5["currency_code"] = "BTC"
				Insert_fields_w5[count_w5] = fields_w5
				trans_array[iStr] = Insert_fields_w5
				count_w5++
			}

			fields := map[string]string{}
			fields["uid"] = uidStr
			if i == 5 {
				fields["currency_code"] = "BTC"
			} else {
				fields["currency_code"] = "USD"
			}
			fields["balance"] = commision_str
			fields["payout_category"] = payoutCategory
			fields["created"] = dateStr
			fields["updated"] = dateStr
			fields["wallet_category"] = iStr
			fields["category"] = category
			fields["transaction_day"] = d
			fields["transaction_month"] = m
			fields["transaction_year"] = y
			fields["transaction_week"] = w
			fields["transaction_date"] = dateCom
			fields["credit_status"] = "1"
			fields["amount_paid"] = commision_str
			fields["associated_user_id"] = associated_user_id
			fields["extra_params"] = dateStr
			fields["common_id"] = common_id
			fields["notes"] = notes

			// // _,err := GoDB.GoInsert(db, "afl_multi_wallet", fields)

			Insert_fields[count] = fields
			// err := MasterWalletTransactionMultiUpdate(db, Insert_fields, payoutCategory, iStr, false)
			// if err != nil {
			// 	return false, err
			// }

			/*bonus_cat := category
			  if(category == "daily_sharing" || category == "coin_sharing") {
			  	bonus_cat = "sharing"
			  }*/

			/*bonusType := strings.ToLower(payoutCategory)
			    	bonusType = strings.Replace(bonusType, " ", "_", -1)
					master_data := map[string]string{}
				    master_data["uid"] = uidStr
				    master_data["amount"] = commision_str
				    master_data["extra_params"] = iStr
				    master_fields[count] = master_data
				    master_cat_fields[wallet_name+"_balance"] = master_fields
				    master_cat_fields[wallet_name+"_"+bonusType] = master_fields*/
			count++
		}
	}

	if len(Insert_fields) > 0 {
		_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet", Insert_fields)
		if err != nil {
			return false, err
		}
	}
	if len(trans_array) > 0 {
		wg := new(sync.WaitGroup)
		ch := make(chan string)
		for x := 1; x <= walletsStr; x++ {
			xStr := strconv.Itoa(x)
			wg.Add(1)
			go AflMasterWalletSplitChannelProcess(db, wg, &ch, trans_array[xStr], payoutCategory, xStr)
		}

		go func(wg *sync.WaitGroup, ch chan string) {
			log.Println("waiting")
			wg.Wait()
			log.Println("done waiting")
			close(ch)
		}(wg, ch)

		for msg := range ch {
			fmt.Println(msg)
		}
	}
	if len(UID_array) > 0 {
		AflPartialUpgradationQueueInsertion(db, UID_array)
	}

	// if len(Insert_fields_w1) > 0 {
	// 	_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_1", Insert_fields_w1)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "1", false)
	// 	err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w1, payoutCategory, "1", false)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// }
	// if len(Insert_fields_w2) > 0 {
	// 	_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_2", Insert_fields_w2)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "2", false)
	// 	err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w2, payoutCategory, "2", false)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// }
	// if len(Insert_fields_w3) > 0 {
	// 	_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_3", Insert_fields_w3)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "3", false)
	// 	err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w3, payoutCategory, "3", false)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	//Partial Upgradation Queue Insertion
	// 	if len(UID_array) > 0 {
	// 		AflPartialUpgradationQueueInsertion(db, UID_array)
	// 	}
	// }
	// if len(Insert_fields_w4) > 0 {
	// 	_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_4", Insert_fields_w4)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "4", false)
	// 	err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w4, payoutCategory, "4", false)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// }
	// if len(Insert_fields_w5) > 0 {
	// 	_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_5", Insert_fields_w5)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "5", false)
	// 	err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w5, payoutCategory, "5", false)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// }

	return true, nil
}

func WeekDayStr(week int) (weekstr string) {
	days := make(map[int]string)
	days[0] = "Sunday"
	days[1] = "Monday"
	days[2] = "Tuesday"
	days[3] = "Wednesday"
	days[4] = "Thursday"
	days[5] = "Friday"
	days[6] = "Saturday"

	weekstr = days[week]
	return
}

func WeekDayInt(week string) (weekint int) {
	days := make(map[int]string)
	days[0] = "Sunday"
	days[1] = "Monday"
	days[2] = "Tuesday"
	days[3] = "Wednesday"
	days[4] = "Thursday"
	days[5] = "Friday"
	days[6] = "Saturday"

	for i, val := range days {
		if val == week {
			weekint = i
		}
	}
	return
}

func ProductSV(db *sql.DB, product_id string) (val string) {

	if product_id != "" {
		val, _ = GoDB.FetchField(db, "field_data_field_business_volume", "field_business_volume_value", "entity_id = "+product_id)
	}
	return val
}
func CheckAdminRecognizedUser(db *sql.DB, uid int) bool {
	if uid > 0 {
		uidstr := strconv.Itoa(uid)
		exists, _ := GoDB.GoRowCount(db, "afl_admin_recognition", "uid = "+uidstr+" AND request_status = 'Approved'")
		if exists > 0 {
			return true
		}
	}
	return false
}
func Member_transaction_multi_function(db *sql.DB, fields map[int]map[string]string, business bool, do_check bool, master bool, bonusType string) (err error) {
	var updated_fields map[int]map[string]string
	updated_fields = make(map[int]map[string]string)
	var master_fields map[int]map[string]string
	master_fields = make(map[int]map[string]string)
	// PR("bonusType=========", bonusType)
	if len(fields) > 0 {
		if do_check == true {
			cond := "( CASE"
			for _, field := range fields {
				cond += " WHEN uid=" + field["uid"] +
					" THEN associated_user_id=" + field["associated_user_id"] + " AND" +
					" category LIKE '" + field["category"] + "' AND" +
					" notes LIKE '" + field["notes"] + "'"
			}
			cond += " END )"

			already_exist, _ := GoDB.FetchAllOrderGroup(db, "afl_user_transactions", "uid", "", cond, "", "", "", "uid")

			for _, uid := range already_exist {
				for key, value := range fields {
					if uid["uid"] == value["uid"] {
						delete(fields, key)
					}
				}
			}
		}
		for key, field := range fields {
			if len(field["merchant_id"]) <= 0 {
				field["merchant_id"] = Marchant_id()
			}
			if len(field["project_name"]) <= 0 {
				field["project_name"] = Project_name()
			}
			balance, _ := strconv.ParseFloat(field["amount_paid"], 64)
			if field["credit_status"] != "1" {
				balance *= -1
			}
			balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
			field["balance"] = balance_str
			updated_fields[key] = field
		}

		if master == true {
			for key, field := range fields {
				master_data := map[string]string{}
				balance, _ := strconv.ParseFloat(field["amount_paid"], 64)
				if field["credit_status"] != "1" {
					balance *= -1
				}
				balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
				master_data["uid"] = field["uid"]
				master_data["amount"] = balance_str
				master_data["extra_params"] = "1"
				// PR("'category' ======",field["category"])
				master_fields[key] = master_data
			}
		}

		_, err = GoDB.GoMultiInsert(db, "afl_user_transactions", updated_fields)
		if err != nil {
			return err
		}
		if len(master_fields) > 0 {
			MasterTransactionMultiUpdate(db, master_fields, "commission_balance", false)
			/*if(bonusType == "daily_sharing" || bonusType == "coin_sharing") {
				bonusType = "sharing"
			}*/
			/*bonusType = strings.ToLower(bonusType)
			bonusType = strings.Replace(bonusType, " ", "_", -1)
			MasterTransactionMultiUpdate(db, master_fields,"commission_"+bonusType, true)*/
		}
	}
	return nil
}
func Member_overall_transaction_multi_function(db *sql.DB, Insfields map[int]map[string]string, business bool, do_check bool, master bool, bonusType string) (err error) {
	var updated_fields map[int]map[string]string
	updated_fields = make(map[int]map[string]string)

	var user_fund_upadate_fields map[int]map[string]string
	user_fund_upadate_fields = make(map[int]map[string]string)

	var user_fund_insert_fields map[int]map[string]string
	user_fund_insert_fields = make(map[int]map[string]string)

	/*var master_fields map[int]map[string]string
	master_fields = make(map[int]map[string]string)*/
	var fields map[int]map[string]string

	if len(Insfields) > 0 {
		if do_check == true {
			cond := "( CASE"
			for _, field := range Insfields {
				cond += " WHEN uid=" + field["uid"] +
					" THEN associated_user_id=" + field["associated_user_id"] + " AND" +
					" category LIKE '" + field["category"] + "' AND" +
					" notes LIKE '" + field["notes"] + "'"
			}
			cond += " END )"

			already_exist, _ := GoDB.FetchAllOrderGroup(db, "afl_user_overall_transaction", "uid", "", cond, "", "", "", "uid")

			var i int

			for _, uid := range Insfields {
				ExistFlag := false
				for _, value := range already_exist {
					if uid["uid"] == value["uid"] {
						ExistFlag = true
					}
				}
				if ExistFlag == false {
					fields[i] = uid
				}

			}
		} else {
			fields = Insfields
		}
		// PR("fields-utility", fields)
		for key, field := range fields {
			/*if len(field["order_id"]) > 0 {
			  delete(field, "order_id")
			}*/
			balance, _ := strconv.ParseFloat(field["amount_paid"], 64)
			// PR("balance",balance)
			if field["credit_status"] != "1" {
				balance *= -1
			}
			balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
			field["balance"] = balance_str
			updated_fields[key] = field
		}

		if master == true {
			/*for key,field := range fields {
			    		master_data := map[string]string{}
					    balance, _ := strconv.ParseFloat(field["amount_paid"], 64)
			    		if field["credit_status"] != "1" {
					        balance *= -1
					    }
					    balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
					    master_data["uid"] = field["uid"]
					    master_data["amount"] = balance_str
					    master_data["extra_params"] = "6"

					    master_fields[key] = master_data
			    	}*/
		}

		_, err = GoDB.GoMultiInsert(db, "afl_user_overall_transaction", updated_fields)
		if err != nil {
			// PR("Member_overall_transaction_multi_function error", err)
			return err
		}
		i := 0
		count := len(fields)
		cond := "uid IN ("
		for _, field := range fields {
			if i++; i < count {
				cond += "'" + field["uid"] + "', "
			} else {
				cond += "'" + field["uid"] + "' "
			}
		}
		cond += ")"

		root := EpsRoot(db)
		rootStr := strconv.Itoa(root)
		uf, _ := GoDB.FetchAll(db, "afl_user_fund", "uid, balance", "", cond, "", "")
		// PR("uf",uf)

		var uf_uids []string
		var fields_uids []string

		for _, value := range uf {
			if In_array(value["uid"], uf_uids) == false {
				uf_uids = append(uf_uids, value["uid"])
			}
		}

		for _, field := range fields {
			if In_array(field["uid"], uf_uids) == false && field["uid"] != rootStr {
				fields_uids = append(fields_uids, field["uid"])
			}
		}

		var up_uid string
		var update map[string]string

		for _, value := range uf {
			for _, field := range fields {
				if field["uid"] == value["uid"] {
					up_uid = field["uid"]
					ex_balance, _ := strconv.ParseFloat(field["amount_paid"], 64)
					if field["credit_status"] != "1" {
						ex_balance *= -1
					}
					if len(update[up_uid]) > 0 {
						exist_balance, _ := strconv.ParseFloat(update[up_uid], 64)
						ex_balance = ex_balance + exist_balance
					}
					existing_balance, _ := strconv.ParseFloat(value["balance"], 64)
					new_balance := ex_balance + existing_balance
					balance_str := strconv.FormatFloat(new_balance, 'f', 3, 64)
					update = make(map[string]string)
					update["uid"] = field["uid"]
					update["balance"] = balance_str
					update["currency_code"] = field["currency_code"]
					update["modified"] = field["created"]
				}
			}
			uidI, _ := strconv.Atoi(up_uid)
			user_fund_upadate_fields[uidI] = update
		}

		i = 0
		if len(fields_uids) > 0 {
			var new_update map[string]string
			for _, new_uid := range fields_uids {
				for _, new_field := range fields {
					if new_field["uid"] == new_uid {
						// ins_uid = field["uid"]
						new_balance, _ := strconv.ParseFloat(new_field["amount_paid"], 64)
						if new_field["credit_status"] != "1" {
							new_balance *= -1
						}
						new_balance_str := strconv.FormatFloat(new_balance, 'f', 3, 64)
						new_update = make(map[string]string)
						new_update["uid"] = new_field["uid"]
						new_update["balance"] = new_balance_str
						new_update["currency_code"] = new_field["currency_code"]
						new_update["modified"] = new_field["created"]
					}
				}
				user_fund_insert_fields[i] = new_update
				i++
			}
		}
		if len(user_fund_upadate_fields) > 0 {
			GoDB.GoMultiUpdate(db, "afl_user_fund", user_fund_upadate_fields, "uid", "", "")
		}
		if len(user_fund_insert_fields) > 0 {
			GoDB.GoMultiInsert(db, "afl_user_fund", user_fund_insert_fields)
		}

		var ins_business_fields map[int]map[string]string
		ins_business_fields = make(map[int]map[string]string)
		var businessFields map[string]string
		i = 0

		if business == true {
			for _, field := range fields {
				businessFields = make(map[string]string)
				balance, _ := strconv.ParseFloat(field["amount_paid"], 64)
				businessFields["associated_user_id"] = field["associated_user_id"]
				businessFields["uid"] = field["uid"]
				if field["credit_status"] == "1" {
					businessFields["credit_status"] = "0"
				} else {
					businessFields["credit_status"] = "1"
					balance *= -1
				}
				if len(field["calc_details"]) > 0 {
					businessFields["calc_details"] = field["calc_details"]
				}
				balance *= -1
				balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
				businessFields["amount_paid"] = field["amount_paid"]
				businessFields["currency_code"] = field["currency_code"]
				businessFields["balance"] = balance_str
				businessFields["category"] = field["category"]
				businessFields["notes"] = field["notes"]
				businessFields["order_id"] = field["order_id"]
				businessFields["transaction_day"] = field["transaction_day"]
				businessFields["transaction_month"] = field["transaction_month"]
				businessFields["transaction_year"] = field["transaction_year"]
				businessFields["transaction_week"] = field["transaction_week"]
				businessFields["transaction_date"] = field["transaction_date"]
				businessFields["created"] = field["created"]
				ins_business_fields[i] = businessFields
				i++
			}
			if len(ins_business_fields) > 0 {
				/*_,errors :=*/ Business_transaction_multi_fuction(db, ins_business_fields, false)
			}
		}
		/*bonusType = strings.ToLower(bonusType)
		  bonusType = strings.Replace(bonusType, " ", "_", -1)

		  if len(master_fields) > 0 {
		  	MasterTransactionMultiUpdate(db, master_fields,"overall_"+bonusType, true)
		  }*/

	}
	return nil
}

func Business_transaction_multi_fuction(db *sql.DB, fields map[int]map[string]string, do_check bool) (status bool, errR error) {
	var result bool
	var err error
	var exist_field string
	InsertArray := map[int]map[string]string{}
	already_exist := map[int]map[string]string{}
	if do_check == true {

		cond := "( CASE "
		for _, field := range fields {
			exist_field = "uid"
			cond += " WHEN uid=" + field["uid"] +
				" THEN associated_user_id=" + field["associated_user_id"] + " AND " +
				" category LIKE '" + field["category"] + "' AND " +
				" notes LIKE '" + field["notes"] + "'"
		}
		cond += " END )"
		already_exist, _ = GoDB.FetchAllOrderGroup(db, "afl_business_transactions", exist_field, "", cond, "", "", "", "uid")
		for _, uid := range already_exist {
			for key, value := range fields {
				if uid["uid"] == value["uid"] {
					delete(fields, key)
				}
			}
		}
	}
	if len(already_exist) <= 0 {
		i := 0
		if len(fields) > 0 {
			for _, field := range fields {
				// afl_date := EpsdateDev(db)
				afl_date := Epsdate(db)
				afl_date_split := Go_date_splits(db, afl_date)
				afl_date_I := int(afl_date)
				afl_date_str := strconv.Itoa(afl_date_I)
				d := strconv.Itoa(afl_date_split["d"])
				m := strconv.Itoa(afl_date_split["m"])
				y := strconv.Itoa(afl_date_split["y"])
				w := strconv.Itoa(afl_date_split["w"])
				combine_d := y + "-" + m + "-" + d

				balance, _ := strconv.ParseFloat(field["amount_paid"], 64)
				if field["credit_status"] != "1" {
					balance *= -1
				}
				balance_str := strconv.FormatFloat(balance, 'f', 3, 64)
				field["balance"] = balance_str
				field["created"] = afl_date_str
				field["transaction_day"] = d
				field["transaction_month"] = m
				field["transaction_year"] = y
				field["transaction_week"] = w
				field["transaction_date"] = combine_d
				if len(field["merchant_id"]) <= 0 {
					field["merchant_id"] = Marchant_id()
				}
				if len(field["project_name"]) <= 0 {
					field["project_name"] = Project_name()
				}
				InsertArray[i] = field
				i += 1

			}
			result, err = GoDB.GoMultiInsert(db, "afl_business_transactions", InsertArray)
		}
	}
	return result, err
}

/*
uid, uid int, amount string, category string, payoutCategory string, date int64, date_splits map[string]int, associated_user_id string
*/

func MultiWalletSplit_MultiInsert(db *sql.DB, fields map[int]map[string]string, bonus_category string) error {
	pr := PR
	// var flag int
	var rate string
	var Insert_fields map[int]map[string]string
	Insert_fields = make(map[int]map[string]string)

	var Insert_fields_w1 map[int]map[string]string
	Insert_fields_w1 = make(map[int]map[string]string)

	var Insert_fields_w2 map[int]map[string]string
	Insert_fields_w2 = make(map[int]map[string]string)

	var Insert_fields_w3 map[int]map[string]string
	Insert_fields_w3 = make(map[int]map[string]string)

	var Insert_fields_w4 map[int]map[string]string
	Insert_fields_w4 = make(map[int]map[string]string)

	var Insert_fields_w5 map[int]map[string]string
	Insert_fields_w5 = make(map[int]map[string]string)
	/*var master_map map[int]map[string]map[string]string
	  master_map = make(map[int]map[string]map[string]string)
	  var master_cat_fields map[string]map[int]map[string]string
	  master_cat_fields = make(map[string]map[int]map[string]string)*/
	/*var categ map[string]string
	  categ = make(map[string]string)*/
	currency := CommerceDeafaultCurrency(db)
	wallets, _ := GoDB.AFLVariableGet(db, "afl_max_wallet")
	walletsStr, _ := strconv.Atoi(wallets)
	count := 0
	count_w1 := 0
	count_w2 := 0
	count_w3 := 0
	count_w4 := 0
	count_w5 := 0
	UID_array := []string{}
	// cond := "uid IN ("
	for _, field := range fields {
		if _, ok := field["amount_paid"]; ok  {
			field["amount"] = field["amount_paid"]
		}
		if _, ok := field["created"]; ok  {
			field["date"] = field["created"]
		}
		if _, ok := field["transaction_day"]; ok {
			field["d"] = field["transaction_day"]
		}
		if _, ok := field["transaction_month"]; ok {
			field["m"] = field["transaction_month"]
		}

		if _, ok := field["transaction_year"]; ok {
			field["y"] = field["transaction_year"]
		}

		if _, ok := field["transaction_week"]; ok {
			field["w"] = field["transaction_week"]
		}
		if _, ok := field["payoutCategory"]; !ok {
			field["payoutCategory"] = field["category"]
		}
		flag := 0
		uidStr := field["uid"]
		var category string
		if _, ok := field["extra_params"]; ok {
			category = field["extra_params"]
		}else{
			category = field["category"]
		}

		payoutCategory := field["payoutCategory"]
		// payoutCategory := field["payoutCategory"]
		associated_user_id := field["associated_user_id"]
		exists, _ := GoDB.GoRowCount(db, "afl_admin_recognition", "uid = "+uidStr+" AND request_status = 'Approved'")
		if exists <= 0 {
			flag = 1
		}
		for i := 1; i <= walletsStr; i++ {
			iStr := strconv.Itoa(i)
			if flag == 1 {
				rate, _ = GoDB.AFLVariableGet(db, "afl_wallet_allocation_"+category+"_"+iStr)
			} else {
				rate, _ = GoDB.AFLVariableGet(db, "afl_wallet_allocation_admin_recognizion_"+iStr)
			}
			// wallet_name, _ := GoDB.AFLVariableGet(db, "afl_wallet_name_"+iStr)
			amount_payable := Commision_amount(rate, field["amount"])

			// amount_strF,_ := strconv.ParseFloat(commision_str, 64)
			if amount_payable > 0.0 {
				commision_str := strconv.FormatFloat(amount_payable, 'f', 6, 64)
				switch iStr {
				case "1":
					data_w1 := map[string]string{}
					data_w1["uid"] = uidStr
					data_w1["currency_code"] = currency
					data_w1["balance"] = commision_str
					data_w1["payout_category"] = payoutCategory
					data_w1["created"] = field["date"]
					data_w1["updated"] = field["date"]
					data_w1["wallet_category"] = iStr
					data_w1["category"] = category
					data_w1["transaction_day"] = field["d"]
					data_w1["transaction_month"] = field["m"]
					data_w1["transaction_year"] = field["y"]
					data_w1["transaction_week"] = field["w"]
					data_w1["transaction_date"] = field["date_combined"]
					data_w1["credit_status"] = "1"
					data_w1["amount_paid"] = commision_str
					data_w1["associated_user_id"] = associated_user_id
					data_w1["extra_params"] = field["date"]
					data_w1["common_id"] = field["common_id"]
					data_w1["notes"] = field["notes"]
					Insert_fields_w1[count_w1] = data_w1
					count_w1++

				case "2":
					data_w2 := map[string]string{}
					data_w2["uid"] = uidStr
					data_w2["currency_code"] = currency
					data_w2["balance"] = commision_str
					data_w2["payout_category"] = payoutCategory
					data_w2["created"] = field["date"]
					data_w2["updated"] = field["date"]
					data_w2["wallet_category"] = iStr
					data_w2["category"] = category
					data_w2["transaction_day"] = field["d"]
					data_w2["transaction_month"] = field["m"]
					data_w2["transaction_year"] = field["y"]
					data_w2["transaction_week"] = field["w"]
					data_w2["transaction_date"] = field["date_combined"]
					data_w2["credit_status"] = "1"
					data_w2["amount_paid"] = commision_str
					data_w2["associated_user_id"] = associated_user_id
					data_w2["extra_params"] = field["date"]
					data_w2["common_id"] = field["common_id"]
					data_w2["notes"] = field["notes"]
					Insert_fields_w2[count_w2] = data_w2
					count_w2++
				case "3":
					UID_array = append(UID_array, uidStr)
					data_w3 := map[string]string{}
					data_w3["uid"] = uidStr
					data_w3["currency_code"] = currency
					data_w3["balance"] = commision_str
					data_w3["payout_category"] = payoutCategory
					data_w3["created"] = field["date"]
					data_w3["updated"] = field["date"]
					data_w3["wallet_category"] = iStr
					data_w3["category"] = category
					data_w3["transaction_day"] = field["d"]
					data_w3["transaction_month"] = field["m"]
					data_w3["transaction_year"] = field["y"]
					data_w3["transaction_week"] = field["w"]
					data_w3["transaction_date"] = field["date_combined"]
					data_w3["credit_status"] = "1"
					data_w3["amount_paid"] = commision_str
					data_w3["associated_user_id"] = associated_user_id
					data_w3["extra_params"] = field["date"]
					data_w3["common_id"] = field["common_id"]
					data_w3["notes"] = field["notes"]
					Insert_fields_w3[count_w3] = data_w3
					count_w3++
				case "4":
					data_w4 := map[string]string{}
					data_w4["uid"] = uidStr
					data_w4["currency_code"] = currency
					data_w4["balance"] = commision_str
					data_w4["payout_category"] = payoutCategory
					data_w4["created"] = field["date"]
					data_w4["updated"] = field["date"]
					data_w4["wallet_category"] = iStr
					data_w4["category"] = category
					data_w4["transaction_day"] = field["d"]
					data_w4["transaction_month"] = field["m"]
					data_w4["transaction_year"] = field["y"]
					data_w4["transaction_week"] = field["w"]
					data_w4["transaction_date"] = field["date_combined"]
					data_w4["credit_status"] = "1"
					data_w4["amount_paid"] = commision_str
					data_w4["associated_user_id"] = associated_user_id
					data_w4["extra_params"] = field["date"]
					data_w4["common_id"] = field["common_id"]
					data_w4["notes"] = field["notes"]
					Insert_fields_w4[count_w4] = data_w4
					count_w4++
				case "5":
					data_w5 := map[string]string{}
					data_w5["uid"] = uidStr
					data_w5["currency_code"] = "BTC"
					data_w5["balance"] = commision_str
					data_w5["payout_category"] = payoutCategory
					data_w5["created"] = field["date"]
					data_w5["updated"] = field["date"]
					data_w5["wallet_category"] = iStr
					data_w5["category"] = category
					data_w5["transaction_day"] = field["d"]
					data_w5["transaction_month"] = field["m"]
					data_w5["transaction_year"] = field["y"]
					data_w5["transaction_week"] = field["w"]
					data_w5["transaction_date"] = field["date_combined"]
					data_w5["credit_status"] = "1"
					data_w5["amount_paid"] = commision_str
					data_w5["associated_user_id"] = associated_user_id
					data_w5["extra_params"] = field["date"]
					data_w5["common_id"] = field["common_id"]
					data_w5["notes"] = field["notes"]
					Insert_fields_w5[count_w5] = data_w5
					count_w5++
				}
				data := map[string]string{}
				data["currency_code"] = currency
				if i == 5 {
					data["currency_code"] = "BTC"
				}
				data["uid"] = uidStr
				data["balance"] = commision_str
				data["payout_category"] = payoutCategory
				data["created"] = field["date"]
				data["updated"] = field["date"]
				data["wallet_category"] = iStr
				data["category"] = category
				data["transaction_day"] = field["d"]
				data["transaction_month"] = field["m"]
				data["transaction_year"] = field["y"]
				data["transaction_week"] = field["w"]
				data["transaction_date"] = field["date_combined"]
				data["credit_status"] = "1"
				data["amount_paid"] = commision_str
				data["associated_user_id"] = associated_user_id
				data["extra_params"] = field["date"]
				data["common_id"] = field["common_id"]
				data["notes"] = field["notes"]
				Insert_fields[count] = data
				count++
				/*bonusType := strings.ToLower(payoutCategory)\
				    	bonusType = strings.Replace(bonusType, " ", "_", -1)
				        master_data := map[string]string{}
				        var master_fields map[string]map[string]string
						master_fields = make(map[string]map[string]string)
				        master_data["uid"] = uidStr
				        master_data["amount"] = commision_str
				        master_data["extra_params"] = iStr
				        master_fields[wallet_name+"_"+bonusType] = master_data
				        master_fields[wallet_name+"_balance"] = master_data
				        master_map[count] = master_fields
				        pr("commision_str 1 --------- ", commision_str)
				        PR("master_map =======", master_map)*/
				// master_cat_fields[wallet_name+"_balance"] = master_fields
				// master_cat_fields[wallet_name+"_"+bonusType] = master_fields
				// count++
				/*if(len(categ[wallet_name+"_"+bonusType]) <= 0 ){
				  	categ[wallet_name+"_"+bonusType] = wallet_name+"_"+bonusType
				  }
				  if(len(categ[wallet_name+"_balance"]) <= 0 ){
				  	categ[wallet_name+"_balance"] = wallet_name+"_balance"
				  }*/
				// if len(Insert_fields[count]) > 0 {
				// 	// err := MasterWalletTransactionMultiUpdate(db, data, payoutCategory, iStr, false)

				// }
			}
		}

	}
	// var tasksWG sync.WaitGroup
	// ch := make(chan map[string]interface{} )

	if len(Insert_fields) > 0 {
		// tasksWG.Add(1)
		// go func() {
  //       	defer tasksWG.Done()
			
			_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet", Insert_fields)
			pr(err)
			// channelval := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields,
			// 	"db":"afl_multi_wallet",
			// }

		// 	ch<-channelval
			
			if err != nil {
				
				return err
			}

		// }()
		// 	//updated on 22-09-2020 =======================
		// 	AflPartialUpgradationQueueInsertion(db, UID_array)
		// 	//=============================================
	}
	if len(Insert_fields_w1) > 0 {
		// tasksWG.Add(1)
		// go func() {
  //       	defer tasksWG.Done()
			_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_1", Insert_fields_w1)
			// channelval1 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w1,
			// 	"db":"afl_multi_wallet_1",
			// }

			// ch<-channelval1
			if err != nil {
				return err
			}
			// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "1", false)
			err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w1, bonus_category, "1", false)
			
			// channelval2 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w1,
			// 	"db":"afl_multi_wallet_1",
			// 	"bonus_category":bonus_category,
			// }

			// ch<-channelval2
			if err != nil {
				return err
			}
		// }()
	}
	if len(Insert_fields_w2) > 0 {
		// tasksWG.Add(1)
		// go func() {
  //       	defer tasksWG.Done()
			_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_2", Insert_fields_w2)
			// channelval1 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w2,
			// 	"db":"afl_multi_wallet_2",
			// }

			// ch<-channelval1
			if err != nil {
				return err
			}
			// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "2", false)
			err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w2, bonus_category, "2", false)
			// channelval2 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w2,
			// 	"db":"afl_multi_wallet_2",
			// 	"bonus_category":bonus_category,
			// }

			// ch<-channelval2
			if err != nil {
				return err
			}
		// }()
	}
	if len(Insert_fields_w3) > 0 {
		// tasksWG.Add(1)
		// go func() {
  //       	defer tasksWG.Done()
			_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_3", Insert_fields_w3)
			// channelval1 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w3,
			// 	"db":"afl_multi_wallet_3",
			// }

			// ch<-channelval1
			if err != nil {
				return err
			}
			err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w3, bonus_category, "3", false)
			// channelval2 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w3,
			// 	"db":"afl_multi_wallet_3",
			// 	"bonus_category":bonus_category,
			// }

			// ch<-channelval2
			// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "3", false)
			if err != nil {
				return err
			}
			//Partial UPgradaion Queue Insertion
			AflPartialUpgradationQueueInsertion(db, UID_array)

		// }()
	}
	if len(Insert_fields_w4) > 0 {
		// tasksWG.Add(1)
		// go func() {
  //       	defer tasksWG.Done()

			_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_4", Insert_fields_w4)

			// channelval1 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w4,
			// 	"db":"afl_multi_wallet_4",
			// }

			// ch<-channelval1
			if err != nil {
				return err
			}
			err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w4, bonus_category, "4", false)
			// channelval2 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w4,
			// 	"db":"afl_multi_wallet_4",
			// 	"bonus_category":bonus_category,
			// }

			// ch<-channelval2
			// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "4", false)
			if err != nil {
				return err
			}
		// }()
	}
	if len(Insert_fields_w5) > 0 {
		// tasksWG.Add(1)
		// go func() {
  //       	defer tasksWG.Done()
			_, err := GoDB.GoMultiInsert(db, "afl_multi_wallet_5", Insert_fields_w5)
			// channelval1 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w5,
			// 	"db":"afl_multi_wallet_5",
			// }

			// ch<-channelval1
			if err != nil {
				return err
			}
			err = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w5, bonus_category, "5", false)
			// channelval2 := map[string]interface{} {
			// 	"err" : err,
			// 	"values" : Insert_fields_w5,
			// 	"db":"afl_multi_wallet_5",
			// 	"bonus_category":bonus_category,
			// }

			// ch<-channelval2
			// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "5", false)
			if err != nil {
				return err
			}
		// }()
	}
	// tasksWG.Wait()
	// for n := range ch {
	// 	fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
		// fmt.Println(ch)
	// 	fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
	// }
	/*if len(master_map) > 0 {
	  	i := 0
	  	for _,cat := range categ {
	  		master_data_map := map[int]map[string]string {}
		  	for _,field_data := range master_map {
		  		if len(field_data[cat]) > 0 {
					master_data_map[i] = field_data[cat]
					i++
				}
		  	}
	  		master_cat_fields[cat] = master_data_map
	  	}
	  }*/

	/*if len(master_cat_fields) > 0 {
		PR("master_cat_fields =========== ", master_cat_fields)
		for cat,field_data := range master_cat_fields {
			if strings.Contains(cat, "_balance") == true {
				MasterTransactionMultiUpdate(db, field_data, cat, false)
			}else{
				MasterTransactionMultiUpdate(db, field_data, cat, true)
			}
		}
	}*/
	return nil
}
// Multi wallet Before DB transaction add

func MultiWalletSplit_MultiInsertQue(db *sql.DB, fields map[int]map[string]string, bonus_category string,transaction_id string) error {
	pr := PR
	// var flag int
	var rate string
	var Insert_fields map[int]map[string]string
	Insert_fields = make(map[int]map[string]string)

	var Insert_fields_w1 map[int]map[string]string
	Insert_fields_w1 = make(map[int]map[string]string)

	var Insert_fields_w2 map[int]map[string]string
	Insert_fields_w2 = make(map[int]map[string]string)

	var Insert_fields_w3 map[int]map[string]string
	Insert_fields_w3 = make(map[int]map[string]string)

	var Insert_fields_w4 map[int]map[string]string
	Insert_fields_w4 = make(map[int]map[string]string)

	var Insert_fields_w5 map[int]map[string]string
	Insert_fields_w5 = make(map[int]map[string]string)

	currency := CommerceDeafaultCurrency(db)
	wallets, _ := GoDB.AFLVariableGet(db, "afl_max_wallet")
	walletsStr, _ := strconv.Atoi(wallets)
	count := 0
	count_w1 := 0
	count_w2 := 0
	count_w3 := 0
	count_w4 := 0
	count_w5 := 0
	UID_array := []string{}
	// cond := "uid IN ("
	for _, field := range fields {
		if _, ok := field["amount_paid"]; ok  {
			field["amount"] = field["amount_paid"]
		}
		if _, ok := field["created"]; ok  {
			field["date"] = field["created"]
		}
		if _, ok := field["transaction_day"]; ok {
			field["d"] = field["transaction_day"]
		}
		if _, ok := field["transaction_month"]; ok {
			field["m"] = field["transaction_month"]
		}

		if _, ok := field["transaction_year"]; ok {
			field["y"] = field["transaction_year"]
		}

		if _, ok := field["transaction_week"]; ok {
			field["w"] = field["transaction_week"]
		}
		if _, ok := field["payoutCategory"]; !ok {
			field["payoutCategory"] = field["category"]
		}
		flag := 0
		uidStr := field["uid"]
		var category string
		if _, ok := field["extra_params"]; ok {
			category = field["extra_params"]
		}else{
			category = field["category"]
		}

		payoutCategory := field["payoutCategory"]
		// payoutCategory := field["payoutCategory"]
		associated_user_id := field["associated_user_id"]
		exists, _ := GoDB.GoRowCount(db, "afl_admin_recognition", "uid = "+uidStr+" AND request_status = 'Approved'")
		if exists <= 0 {
			flag = 1
		}
		for i := 1; i <= walletsStr; i++ {
			iStr := strconv.Itoa(i)
			if flag == 1 {
				rate, _ = GoDB.AFLVariableGet(db, "afl_wallet_allocation_"+category+"_"+iStr)
			} else {
				rate, _ = GoDB.AFLVariableGet(db, "afl_wallet_allocation_admin_recognizion_"+iStr)
			}
			// wallet_name, _ := GoDB.AFLVariableGet(db, "afl_wallet_name_"+iStr)
			amount_payable := Commision_amount(rate, field["amount"])

			// amount_strF,_ := strconv.ParseFloat(commision_str, 64)
			if amount_payable > 0.0 {
				commision_str := strconv.FormatFloat(amount_payable, 'f', 6, 64)
				switch iStr {
				case "1":
					data_w1 := map[string]string{}
					data_w1["uid"] = uidStr
					data_w1["currency_code"] = currency
					data_w1["balance"] = commision_str
					data_w1["payout_category"] = payoutCategory
					data_w1["created"] = field["date"]
					data_w1["updated"] = field["date"]
					data_w1["wallet_category"] = iStr
					data_w1["category"] = category
					data_w1["transaction_day"] = field["d"]
					data_w1["transaction_month"] = field["m"]
					data_w1["transaction_year"] = field["y"]
					data_w1["transaction_week"] = field["w"]
					data_w1["transaction_date"] = field["date_combined"]
					data_w1["credit_status"] = "1"
					data_w1["amount_paid"] = commision_str
					data_w1["associated_user_id"] = associated_user_id
					data_w1["extra_params"] = field["date"]
					data_w1["common_id"] = field["common_id"]
					data_w1["notes"] = field["notes"]
					Insert_fields_w1[count_w1] = data_w1
					count_w1++

				case "2":
					data_w2 := map[string]string{}
					data_w2["uid"] = uidStr
					data_w2["currency_code"] = currency
					data_w2["balance"] = commision_str
					data_w2["payout_category"] = payoutCategory
					data_w2["created"] = field["date"]
					data_w2["updated"] = field["date"]
					data_w2["wallet_category"] = iStr
					data_w2["category"] = category
					data_w2["transaction_day"] = field["d"]
					data_w2["transaction_month"] = field["m"]
					data_w2["transaction_year"] = field["y"]
					data_w2["transaction_week"] = field["w"]
					data_w2["transaction_date"] = field["date_combined"]
					data_w2["credit_status"] = "1"
					data_w2["amount_paid"] = commision_str
					data_w2["associated_user_id"] = associated_user_id
					data_w2["extra_params"] = field["date"]
					data_w2["common_id"] = field["common_id"]
					data_w2["notes"] = field["notes"]
					Insert_fields_w2[count_w2] = data_w2
					count_w2++
				case "3":
					UID_array = append(UID_array, uidStr)
					data_w3 := map[string]string{}
					data_w3["uid"] = uidStr
					data_w3["currency_code"] = currency
					data_w3["balance"] = commision_str
					data_w3["payout_category"] = payoutCategory
					data_w3["created"] = field["date"]
					data_w3["updated"] = field["date"]
					data_w3["wallet_category"] = iStr
					data_w3["category"] = category
					data_w3["transaction_day"] = field["d"]
					data_w3["transaction_month"] = field["m"]
					data_w3["transaction_year"] = field["y"]
					data_w3["transaction_week"] = field["w"]
					data_w3["transaction_date"] = field["date_combined"]
					data_w3["credit_status"] = "1"
					data_w3["amount_paid"] = commision_str
					data_w3["associated_user_id"] = associated_user_id
					data_w3["extra_params"] = field["date"]
					data_w3["common_id"] = field["common_id"]
					data_w3["notes"] = field["notes"]
					Insert_fields_w3[count_w3] = data_w3
					count_w3++
				case "4":
					data_w4 := map[string]string{}
					data_w4["uid"] = uidStr
					data_w4["currency_code"] = currency
					data_w4["balance"] = commision_str
					data_w4["payout_category"] = payoutCategory
					data_w4["created"] = field["date"]
					data_w4["updated"] = field["date"]
					data_w4["wallet_category"] = iStr
					data_w4["category"] = category
					data_w4["transaction_day"] = field["d"]
					data_w4["transaction_month"] = field["m"]
					data_w4["transaction_year"] = field["y"]
					data_w4["transaction_week"] = field["w"]
					data_w4["transaction_date"] = field["date_combined"]
					data_w4["credit_status"] = "1"
					data_w4["amount_paid"] = commision_str
					data_w4["associated_user_id"] = associated_user_id
					data_w4["extra_params"] = field["date"]
					data_w4["common_id"] = field["common_id"]
					data_w4["notes"] = field["notes"]
					Insert_fields_w4[count_w4] = data_w4
					count_w4++
				case "5":
					data_w5 := map[string]string{}
					data_w5["uid"] = uidStr
					data_w5["currency_code"] = "BTC"
					data_w5["balance"] = commision_str
					data_w5["payout_category"] = payoutCategory
					data_w5["created"] = field["date"]
					data_w5["updated"] = field["date"]
					data_w5["wallet_category"] = iStr
					data_w5["category"] = category
					data_w5["transaction_day"] = field["d"]
					data_w5["transaction_month"] = field["m"]
					data_w5["transaction_year"] = field["y"]
					data_w5["transaction_week"] = field["w"]
					data_w5["transaction_date"] = field["date_combined"]
					data_w5["credit_status"] = "1"
					data_w5["amount_paid"] = commision_str
					data_w5["associated_user_id"] = associated_user_id
					data_w5["extra_params"] = field["date"]
					data_w5["common_id"] = field["common_id"]
					data_w5["notes"] = field["notes"]
					Insert_fields_w5[count_w5] = data_w5
					count_w5++
				}
				data := map[string]string{}
				data["currency_code"] = currency
				if i == 5 {
					data["currency_code"] = "BTC"
				}
				data["uid"] = uidStr
				data["balance"] = commision_str
				data["payout_category"] = payoutCategory
				data["created"] = field["date"]
				data["updated"] = field["date"]
				data["wallet_category"] = iStr
				data["category"] = category
				data["transaction_day"] = field["d"]
				data["transaction_month"] = field["m"]
				data["transaction_year"] = field["y"]
				data["transaction_week"] = field["w"]
				data["transaction_date"] = field["date_combined"]
				data["credit_status"] = "1"
				data["amount_paid"] = commision_str
				data["associated_user_id"] = associated_user_id
				data["extra_params"] = field["date"]
				data["common_id"] = field["common_id"]
				data["notes"] = field["notes"]
				Insert_fields[count] = data
				count++
				
			}
		}

	}


	if len(Insert_fields) > 0 {
		var err1 error
		check1 := TanxBlockCheck(db,fmt.Sprintf("multi_wallet_%s",transaction_id),"transx_"+transaction_id)
		if check1 == false {

			_, err1 = GoDB.GoMultiInsert(db, "afl_multi_wallet", Insert_fields)
			if err1 == nil {
				TanxBlockAdd(db,fmt.Sprintf("multi_wallet_%s",transaction_id),"transx_"+transaction_id)
			}else {
				return err1
			}
		}else{
			pr("Alresdy Exist : ",fmt.Sprintf("multi_wallet_%s",transaction_id))
		}

		pr(err1)
	}
	if len(Insert_fields_w1) > 0 {
			var err2 error
			check2 := TanxBlockCheck(db,fmt.Sprintf("multi_wallet_1_%s",transaction_id),"transx_"+transaction_id)
			if check2 == false {
				_, err2 = GoDB.GoMultiInsert(db, "afl_multi_wallet_1", Insert_fields_w1)
				if err2 == nil {
					TanxBlockAdd(db,fmt.Sprintf("multi_wallet_1_%s",transaction_id),"transx_"+transaction_id)
				}else {
					return err2
				}
			}else{
				pr("Alresdy Exist : ",fmt.Sprintf("multi_wallet_1_%s",transaction_id))
			}
			var err3 error
			check3 := TanxBlockCheck(db,fmt.Sprintf("Update_with_Category_1_%s",transaction_id),"transx_"+transaction_id)
			if check3 == false {
				err3 = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w1, bonus_category, "1", false)
				pr("err3 err3 err3 : ",err3)
				if err3 == nil {
					TanxBlockAdd(db,fmt.Sprintf("Update_with_Category_1_%s",transaction_id),"transx_"+transaction_id)
				}else{
					return err3
				}
			}else{
				pr("Alresdy Exist : ",fmt.Sprintf("Update_with_Category_1_%s",transaction_id))
			}	
	}
	pr("Insert_fields_w2 : ",Insert_fields_w2)
	if len(Insert_fields_w2) > 0 {
			var err4 error
			check4 := TanxBlockCheck(db,fmt.Sprintf("multi_wallet_2_%s",transaction_id),"transx_"+transaction_id)
			
			if check4 == false {
				_, err4 = GoDB.GoMultiInsert(db, "afl_multi_wallet_2", Insert_fields_w2)
				
				if err4 != nil {
					TanxBlockAdd(db,fmt.Sprintf("multi_wallet_2_%s",transaction_id),"transx_"+transaction_id)
				}else {
					return err4
				}

			}else{
				pr("Alresdy Exist : ",fmt.Sprintf("multi_wallet_2_%s",transaction_id))
			}
			var err42 error
			check5 := TanxBlockCheck(db,fmt.Sprintf("Update_with_Category_2_%s",transaction_id),"transx_"+transaction_id)
			if check5 == false {
				err42 = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w2, bonus_category, "2", false)
				if err42 != nil {
					TanxBlockAdd(db,fmt.Sprintf("Update_with_Category_2_%s",transaction_id),"transx_"+transaction_id)
				}else {
					return err42
				}
			}else{
				pr("Alresdy Exist : ",fmt.Sprintf("Update_with_Category_2_%s",transaction_id))
			}
			
	}
	pr("Insert_fields_w3 : ",Insert_fields_w3)
	if len(Insert_fields_w3) > 0 {
		var err5 error
		check6 := TanxBlockCheck(db,fmt.Sprintf("multi_wallet_3_%s",transaction_id),"transx_"+transaction_id)
		pr("Insert_fields_w3 check6 : ",check6)
		if check6 == false {
			_, err5 = GoDB.GoMultiInsert(db, "afl_multi_wallet_3", Insert_fields_w3)

			if err5 == nil {
				TanxBlockAdd(db,fmt.Sprintf("multi_wallet_3_%s",transaction_id),"transx_"+transaction_id)
			}else {
				return err5
			}
		}else{
			pr("Alresdy Exist : ",fmt.Sprintf("multi_wallet_3_%s",transaction_id))
		}
		var err52 error
		check7 := TanxBlockCheck(db,fmt.Sprintf("Update_with_Category_3_%s",transaction_id),"transx_"+transaction_id)
		if check7 == false {
			err52 = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w3, bonus_category, "3", false)
			
			if err52 == nil {
				TanxBlockAdd(db,fmt.Sprintf("Update_with_Category_3_%s",transaction_id),"transx_"+transaction_id)
				AflPartialUpgradationQueueInsertion(db, UID_array)

			}else {
				return err52
			}
		}else{
			pr("Alresdy Exist : ",fmt.Sprintf("Update_with_Category_3_%s",transaction_id))
		}

	}
	pr("Insert_fields_w4 : ",Insert_fields_w4)
	if len(Insert_fields_w4) > 0 {
		var err6 error
		check8 := TanxBlockCheck(db,fmt.Sprintf("multi_wallet_4_%s",transaction_id),"transx_"+transaction_id)
		if check8 == false {
			_, err6 = GoDB.GoMultiInsert(db, "afl_multi_wallet_4", Insert_fields_w4)
			if err6 == nil {
				TanxBlockAdd(db,fmt.Sprintf("multi_wallet_4_%s",transaction_id),"transx_"+transaction_id)

			}else {
				return err6
			}
		}else{
			pr("Alresdy Exist : ",fmt.Sprintf("multi_wallet_4_%s",transaction_id))
		}
		var err62 error
		check9 := TanxBlockCheck(db,fmt.Sprintf("Update_with_Category_4_%s",transaction_id),"transx_"+transaction_id)
		if check9 == false {

			err62 = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w4, bonus_category, "4", false)
			
			if err62 == nil {
				TanxBlockAdd(db,fmt.Sprintf("Update_with_Category_4_%s",transaction_id),"transx_"+transaction_id)

			}else {
				return err62
			}
		}else{
			pr("Alresdy Exist : ",fmt.Sprintf("Update_with_Category_4_%s",transaction_id))
		}
	}
	pr("Insert_fields_w5 : ",Insert_fields_w5)
	if len(Insert_fields_w5) > 0 {

		var err7 error
		check9 := TanxBlockCheck(db,fmt.Sprintf("multi_wallet_5_%s",transaction_id),"transx_"+transaction_id)

		if check9 == false {
			_, err7 = GoDB.GoMultiInsert(db, "afl_multi_wallet_5", Insert_fields_w5)

			if err7 == nil {
				TanxBlockAdd(db,fmt.Sprintf("multi_wallet_5_%s",transaction_id),"transx_"+transaction_id)

			}else {
				return err7
			}
		}else{
			pr("Alresdy Exist : ",fmt.Sprintf("multi_wallet_5_%s",transaction_id))
		}
		var err72 error
		check10 := TanxBlockCheck(db,fmt.Sprintf("Update_with_Category_5_%s",transaction_id),"transx_"+transaction_id)
		if check10 == false {

			err72 = MasterWalletTransactionMultiUpdate_with_Category(db, Insert_fields_w5, bonus_category, "5", false)
			
			if err72 == nil {
				TanxBlockAdd(db,fmt.Sprintf("Update_with_Category_5_%s",transaction_id),"transx_"+transaction_id)

			}else {
				return err72
			}
		}else{
			pr("Alresdy Exist : ",fmt.Sprintf("Update_with_Category_5_%s",transaction_id))
		}
		
	}
	
	return nil
}

func MasterTransactionUpdate(db *sql.DB, uid int, category string, amount string, byDate bool, extra_params string) {
	// PR("MasterTransactionUpdate cat--" + category)
	// PR("MasterTransactionUpdate amount--" +amount);
	afl_date := Epsdate(db)
	afl_date_split := Go_date_splits(db, afl_date)
	afl_date_I := int(afl_date)
	afl_date_str := strconv.Itoa(afl_date_I)

	d := strconv.Itoa(afl_date_split["d"])
	m := strconv.Itoa(afl_date_split["m"])
	y := strconv.Itoa(afl_date_split["y"])
	w := strconv.Itoa(afl_date_split["w"])

	table := "afl_master_user_transactions"
	cond := " uid = " + strconv.Itoa(uid)
	cond += " AND category = '" + category + "'"
	if byDate == true {
		cond += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
	}

	TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)
	dateStr := time.Unix(afl_date, 0).In(location).Format("2006-01-02 00:00:00")
	date := Strtotime(db, dateStr)
	dateI := int(date)
	common_id := strconv.Itoa(dateI)

	exist, _ := GoDB.FetchField(db, table, "uid", cond)
	fields := map[string]string{}

	fields["updated_on"] = afl_date_str
	fields["updated_day"] = d
	fields["updated_month"] = m
	fields["updated_year"] = y
	fields["updated_week"] = w
	fields["updated_date"] = y + "-" + m + "-" + d
	fields["common_id"] = common_id

	if len(exist) > 0 {
		cond = " uid = " + strconv.Itoa(uid)
		cond += " AND category = '" + category + "'"
		if byDate == true {
			cond += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
		}
		_, _ = GoDB.GoUpdate(db, table, fields, cond, ", value=value+"+amount)

	} else {
		fields["uid"] = strconv.Itoa(uid)
		fields["category"] = category
		fields["value"] = amount
		fields["created_on"] = afl_date_str
		fields["extra_params"] = extra_params
		_, _ = GoDB.GoInsert(db, table, fields)
	}

}

func MasterTransactionMultiUpdate(db *sql.DB, fields map[int]map[string]string, category string, byDate bool) error {
	// PR("MasterTransactionMultiUpdate cat--" + category)
	// PR("MasterTransactionMultiUpdate fields--" , fields);
	afl_date := Epsdate(db)
	afl_date_split := Go_date_splits(db, afl_date)
	afl_date_I := int(afl_date)
	afl_date_str := strconv.Itoa(afl_date_I)
	category_id := GetMasterCategoryID(category)
	// PR("MasterTransactionMultiUpdate datesplits", afl_date_split)
	d := strconv.Itoa(afl_date_split["d"])
	m := strconv.Itoa(afl_date_split["m"])
	y := strconv.Itoa(afl_date_split["y"])
	w := strconv.Itoa(afl_date_split["w"])

	currency := CommerceDeafaultCurrency(db)

	var uids []string
	uids = make([]string, len(fields))
	i := 0
	for _, data := range fields {
		if data["uid"] != "" && data["uid"] != "0" {
			uids[i] = data["uid"]
			i++
		}
	}

	if len(uids) > 0 {
		UidChunk := ChunkArray(uids, 50)
		Or := " ("
		ChunkCount := len(UidChunk)
		var j int
		for _, val := range UidChunk {
			if j++; j < ChunkCount {
				Or += "uid IN (" + strings.Join(val, ",") + ") OR "
			} else {
				Or += "uid IN (" + strings.Join(val, ",") + ")"
			}

		}
		Or += " ) "
		if len(category) > 0 {
			//check the existing users from the master table
			cond := Or
			cond += " AND  category = '" + category + "'"
			if byDate == true {
				cond += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
			}
			existing_uids, _ := GoDB.FetchCol(db, "afl_master_user_transactions", "uid", "", cond)
			non_existing_uids := Array_diff(uids, existing_uids)
			existing_uids_unique := RemoveDulipicates(existing_uids)
			non_existing_uids_unique := RemoveDulipicates(non_existing_uids)
			// PR("existing uids ", existing_uids_unique)
			// PR("non_existing_uids ", non_existing_uids_unique)
			//Get the non existing users
			var mT map[string]string

			if len(existing_uids_unique) > 0 {
				for _, UidD := range existing_uids_unique {
					// var amount float64
					amount := 0.0
					for _, data := range fields {
						if data["uid"] == UidD {
							// PR("update data", data)
							amountF, _ := strconv.ParseFloat(data["amount"], 64)
							amount = amount + amountF
							// PR("update amount", amount)
						}
					}
					cond_str := "uid = " + UidD
					cond_str += " AND category = '" + category + "'"
					if byDate == true {
						cond_str += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
					}
					amountStr := strconv.FormatFloat(amount, 'f', -1, 64)
					GoDB.GoUpdate(db, "afl_master_user_transactions", mT, cond_str, "value = value + "+amountStr)
				}
			}

			if len(non_existing_uids_unique) > 0 {
				var fieldArr map[int]map[string]string
				fieldArr = make(map[int]map[string]string, 0)
				var i int
				TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
				location, _ := time.LoadLocation(TIMEZONE)
				dateStr := time.Unix(afl_date, 0).In(location).Format("2006-01-02 00:00:00")
				date := Strtotime(db, dateStr)
				dateI := int(date)
				common_id := strconv.Itoa(dateI)
				for _, NonUidD := range non_existing_uids_unique {
					var amount float64
					amount = 0.0
					for _, data := range fields {
						if data["uid"] == NonUidD {
							amountF, _ := strconv.ParseFloat(data["amount"], 64)
							amount = amount + amountF
							// PR("insert data", data)
							// PR("insert amount", amount)
							PR(amount)
						}
					}
					if amount != 0.0 {
						amountStr := strconv.FormatFloat(amount, 'f', -1, 64)
						tmp := make(map[string]string, 11)
						tmp["uid"] = NonUidD
						tmp["category"] = category
						tmp["value"] = amountStr
						tmp["extra_params"] = category_id
						tmp["created_on"] = afl_date_str
						tmp["updated_on"] = afl_date_str
						tmp["updated_day"] = d
						tmp["updated_month"] = m
						tmp["updated_year"] = y
						tmp["updated_week"] = w
						tmp["updated_date"] = y + "-" + m + "-" + d
						tmp["currency_code"] = currency
						tmp["common_id"] = common_id
						fieldArr[i] = tmp
						i++
					}
				}
				PR("fieldArr", fieldArr)
				if len(fieldArr) > 0 {
					_, err := GoDB.GoMultiInsert(db, "afl_master_user_transactions", fieldArr)
					PR("MasterTransactionMultiUpdate", err)
				}
			}
		}
	}
	return nil
}

func RemoveDulipicates(intSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range intSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func GetMasterCategoryID(category string) (id string) {
	if strings.Contains(category, "commision") {
		id = "1"
	} else if strings.Contains(category, "S-Wallet") {
		id = "2"
	} else if strings.Contains(category, "U-Wallet") {
		id = "3"
	} else if strings.Contains(category, "L-Wallet") {
		id = "4"
	} else if strings.Contains(category, "BTC-Wallet") {
		id = "5"
	} else if strings.Contains(category, "overall-Wallet") {
		id = "6"
	}
	return id
}
func GetDownlines(db *sql.DB, uid int) map[int]map[string]string {

	var data map[int]map[string]string
	//Get uid
	uid_str := strconv.Itoa(uid)

	//Fields
	fields := `nested_set_referal.node_id,afl_user_genealogy.enrolment_package_id`

	//Conditions
	condN := " node_id = " + uid_str
	node, _ := GoDB.FetchAssoc(db, "nested_set_referal", "", "", condN)
	if len(node) <= 0 {
		return data
	}

	cond := ""
	left := node["lft"]
	right := node["rgt"]
	cond = " nested_set_referal.lft > " + left + " AND nested_set_referal.rgt < " + right

	//Joins
	joins := ""
	joins += "LEFT JOIN afl_user_genealogy ON afl_user_genealogy.uid = nested_set_referal.node_id "
	// joins += "LEFT JOIN afl_product_compensation_attributes ON afl_product_compensation_attributes.product_id = afl_user_genealogy.enrolment_package_id "

	//Fetch Fields
	data, _ = GoDB.FetchAllOrder(db, "nested_set_referal", fields, joins, cond, "0", "", "nested_set_referal.rgt-nested_set_referal.lft ASC")
	PR(data)
	return data

}
func AflPartialUpgradationQueueInsertion(db *sql.DB, uid_str []string) {

	var cond string
	var walletData map[int]map[string]string
	walletData = make(map[int]map[string]string)
	count := 0
	date := Epsdate(db)
	dateI := int(date)
	dateStr := strconv.Itoa(dateI)
	date_splits := Go_date_splits(db, date)
	d := strconv.Itoa(date_splits["d"])
	m := strconv.Itoa(date_splits["m"])
	y := strconv.Itoa(date_splits["y"])
	dateCom := y + "-" + m + "-" + d
	upgradation_base, _ := GoDB.AFLVariableGet(db, "upgradation_base")
	upgradation_baseI, _ := strconv.Atoi(upgradation_base)
	up_base_int := upgradation_baseI * 100
	up_base_float := float64(up_base_int)
	up_base_str := strconv.FormatFloat(up_base_float, 'f', -1, 64)
	if len(uid_str) > 0 {
		UidChunk := ChunkArray(uid_str, 50)
		Or_s := " ("
		ChunkCount := len(UidChunk)
		var i int

		for _, val := range UidChunk {
			if i++; i < ChunkCount {
				Or_s += "uid IN (" + strings.Join(val, ",") + ") OR "
			} else {
				Or_s += "uid IN (" + strings.Join(val, ",") + ")"
			}
		}
		Or_s += " ) "
		cond = Or_s
	}
	Uidcond := cond
	eligbleUsers, _ := GoDB.FetchAllOrderGroup(db, "afl_master_multi_wallet_3", "uid,SUM(balance)", "", Uidcond, "", "", "", "uid HAVING SUM(balance) >= "+up_base_str)
	if len(eligbleUsers) > 0 {
		for _, val := range eligbleUsers {

			fields := map[string]string{}
			fields["uid"] = val["uid"]
			fields["created"] = dateStr
			fields["wallet_category"] = "3"
			fields["transaction_date"] = dateCom
			fields["status"] = "0"
			fields["cron_attempt"] = "0"
			walletData[count] = fields
			count++

		}
		if len(walletData) > 0 {
			_, err := GoDB.GoMultiInsert(db, "afl_partial_upgradation_queue", walletData)
			fmt.Println(err)
		}
	}
}
func Timetounix(db *sql.DB, timestamp int64) time.Time {
	TIMEZONE, _ := GoDB.AFLVariableGet(db, "date_default_timezone")
	location, _ := time.LoadLocation(TIMEZONE)
	return time.Unix(timestamp, 0).In(location)
}
func MasterWalletTransactionUpdate(db *sql.DB, uid int, category string, amount string, wallet_category string, byDate bool) (err error) {
	// os.Exit(123)
	// PR("MasterTransactionUpdate cat--" + category)
	// PR("MasterTransactionUpdate amount--" +amount);
	afl_date := Epsdate(db)
	afl_date_split := Go_date_splits(db, afl_date)
	afl_date_I := int(afl_date)
	afl_date_str := strconv.Itoa(afl_date_I)

	d := strconv.Itoa(afl_date_split["d"])
	m := strconv.Itoa(afl_date_split["m"])
	y := strconv.Itoa(afl_date_split["y"])
	w := strconv.Itoa(afl_date_split["w"])

	table := "afl_master_multi_wallet_" + wallet_category
	cond := " uid = " + strconv.Itoa(uid)
	cond += " AND payout_category = '" + category + "'"
	cond += " AND wallet_category = '" + wallet_category + "'"
	if byDate == true {
		cond += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
	}

	// TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")
	// location, _ := time.LoadLocation(TIMEZONE)
	// dateStr := time.Unix(afl_date, 0).In(location).Format("2006-01-02 00:00:00")
	// date := Strtotime(db, dateStr)
	// dateI := int(date)
	// common_id := strconv.Itoa(dateI)

	exist, _ := GoDB.FetchField(db, table, "uid", cond)
	fields := map[string]string{}

	fields["updated_on"] = afl_date_str
	fields["updated_day"] = d
	fields["updated_month"] = m
	fields["updated_year"] = y
	fields["updated_week"] = w
	fields["updated_date"] = y + "-" + m + "-" + d
	// fields["common_id"] = common_id

	if len(exist) > 0 {
		cond = " uid = " + strconv.Itoa(uid)
		cond += " AND payout_category = '" + category + "'"
		cond += " AND wallet_category = '" + wallet_category + "'"
		if byDate == true {
			cond += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
		}
		_, err = GoDB.GoUpdate(db, table, fields, cond, ", balance=balance+"+amount)
		if err != nil {
			return err
		}

	} else {
		fields["uid"] = strconv.Itoa(uid)
		fields["payout_category"] = category
		fields["wallet_category"] = wallet_category
		fields["balance"] = amount
		fields["created"] = afl_date_str
		// fields["extra_params"] = extra_params
		_, _ = GoDB.GoInsert(db, table, fields)
	}
	return nil
}

func MasterWalletTransactionMultiUpdate(db *sql.DB, fields map[int]map[string]string, wallet_category string, byDate bool) (err error) {
	masterTb := "afl_master_multi_wallet_" + wallet_category

	afl_date := Epsdate(db)
	afl_date_split := Go_date_splits(db, afl_date)
	afl_date_I := int(afl_date)
	afl_date_str := strconv.Itoa(afl_date_I)

	var Insert_fields map[int]map[string]string
	Insert_fields = make(map[int]map[string]string)

	d := strconv.Itoa(afl_date_split["d"])
	m := strconv.Itoa(afl_date_split["m"])
	y := strconv.Itoa(afl_date_split["y"])
	w := strconv.Itoa(afl_date_split["w"])

	count := 0
	var cond string
	if len(fields) > 0 {
		// UidChunk := ChunkArray(uids, 50)
		// Or := " ("
		// ChunkCount := len(UidChunk)
		// var j int
		// for _, val := range UidChunk {
		// 	if j++; j < ChunkCount {
		// 		Or += "uid IN (" + strings.Join(val, ",") + ") OR "
		// 	} else {
		// 		Or += "uid IN (" + strings.Join(val, ",") + ")"
		// 	}

		// }
		// Or += " ) "
		for _, data := range fields {
			//check the existing users from the master table
			// cond := Or
			cond = "uid = " + data["uid"]
			cond += " AND  payout_category = '" + data["payout_category"] + "'"
			cond += " AND  wallet_category = '" + data["wallet_category"] + "'"
			if byDate == true {
				cond += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
			}
			existing_uids, _ := GoDB.FetchCol(db, masterTb, "uid", "", cond)
			// non_existing_uids := Array_diff(uids, existing_uids)
			// existing_uids_unique := RemoveDulipicates(existing_uids)
			// non_existing_uids_unique := RemoveDulipicates(non_existing_uids)
			var mT map[string]string

			if len(existing_uids) > 0 {
				// for _, UidD := range existing_uids_unique {
				// var amount float64
				// amount := 0.0

				// if data["uid"] == UidD {
				// 	// PR("update data", data)
				// 	amountF, _ := strconv.ParseFloat(data["balance"], 64)
				// 	amount = amount + amountF
				// 	// PR("update amount", amount)
				// }
				cond_str := "uid = " + data["uid"]
				cond_str += " AND payout_category = '" + data["payout_category"] + "'"
				cond_str += " AND wallet_category = '" + data["wallet_category"] + "'"
				if byDate == true {
					cond_str += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
				}
				// amountStr := strconv.FormatFloat(amount, 'f', -1, 64)
				// GoDB.GoUpdate(db, "afl_master_multi_wallet", mT, cond_str, "balance = balance + "+data["balance"])
				GoDB.GoUpdate(db, masterTb, mT, cond_str, "balance = balance + "+data["balance"])
				// }
			} else {
				temp_field := map[string]string{}
				temp_field["uid"] = data["uid"]
				temp_field["payout_category"] = data["payout_category"]
				temp_field["wallet_category"] = data["wallet_category"]
				temp_field["balance"] = data["balance"]
				// temp_field["extra_params"] = category_id
				temp_field["created"] = data["created"]
				temp_field["updated_on"] = afl_date_str
				temp_field["updated_day"] = d
				temp_field["updated_month"] = m
				temp_field["updated_year"] = y
				temp_field["updated_week"] = w
				temp_field["updated_date"] = y + "-" + m + "-" + d

				Insert_fields[count] = temp_field
				count++
				// }

				// }
			}
		}
		if len(Insert_fields) > 0 {
			_, err := GoDB.GoMultiInsert(db, masterTb, Insert_fields)
			PR("MasterTransactionMultiUpdate", err)
		}
	}
	return err
}
func MasterWalletTransactionMultiUpdate_with_Category(db *sql.DB, fields map[int]map[string]string, payout_category string, wallet_category string, byDate bool) (err error) {

	masterTb := "afl_master_multi_wallet_" + wallet_category

	afl_date := Epsdate(db)
	afl_date_split := Go_date_splits(db, afl_date)
	afl_date_I := int(afl_date)
	afl_date_str := strconv.Itoa(afl_date_I)
	d := strconv.Itoa(afl_date_split["d"])
	m := strconv.Itoa(afl_date_split["m"])
	y := strconv.Itoa(afl_date_split["y"])
	w := strconv.Itoa(afl_date_split["w"])

	var uids []string
	uids = make([]string, len(fields))
	i := 0
	for _, data := range fields {
		if data["uid"] != "" && data["uid"] != "0" {
			uids[i] = data["uid"]
			i++
		}
	}
	if len(uids) > 0 {
		UidChunk := ChunkArray(uids, 50)
		Or := " ("
		ChunkCount := len(UidChunk)
		var j int
		for _, val := range UidChunk {
			if j++; j < ChunkCount {
				Or += "uid IN (" + strings.Join(val, ",") + ") OR "
			} else {
				Or += "uid IN (" + strings.Join(val, ",") + ")"
			}

		}
		Or += " ) "
		if len(payout_category) > 0 {
			cond := Or
			cond += " AND  payout_category = '" + payout_category + "'"
			cond += " AND  wallet_category = '" + wallet_category + "'"
			if byDate == true {
				cond += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
			}
			existing_uids, _ := GoDB.FetchCol(db, masterTb, "uid", "", cond)
			non_existing_uids := Array_diff(uids, existing_uids)
			existing_uids_unique := RemoveDulipicates(existing_uids)
			non_existing_uids_unique := RemoveDulipicates(non_existing_uids)

			if len(existing_uids_unique) > 0 {

				uid__data_map := make(map[string]string, 0)
				total_vol_fields := make(map[string]interface{}, 0)
				total_vol_arr := make(map[string]interface{}, 0)
				MASTERUPDATEARRAy := make(map[string]interface{}, 0)
				MasterUpdateCondArr := ""
				var total_sum float64
				var amountF float64
				// for _, UidD := range existing_uids_unique {
				// 	// var amount float64
				// 	// amount := 0.0
				// 	for _, data := range fields {
				// 		if data["uid"] == UidD {
				// 			// PR("update data", data)
				// 			// amountF, _ := strconv.ParseFloat(data["balance"], 64)
				// 			// amount = amount + amountF
				// 			// PR("update amount", amount)
				// 			amountStr = data["balance"]
				// 		}
				// 	}
				// 	cond_str := "uid = " + UidD
				// 	cond_str += " AND payout_category = '" + payout_category + "'"
				// 	cond_str += " AND wallet_category = '" + wallet_category + "'"
				// 	if byDate == true {
				// 		cond_str += " AND updated_day =" + d + " AND updated_month = " + m + " AND updated_year = " + y
				// 	}
				// 	// amountStr := strconv.FormatFloat(amount, 'f', -1, 64)
				// 	GoDB.GoUpdate(db, masterTb, mT, cond_str, "balance = balance + "+amountStr)
				// }
				UidChunk := ChunkArray(existing_uids_unique, 50)
				Or := " ("
				ChunkCount := len(UidChunk)
				var j int
				for _, val := range UidChunk {
					if j++; j < ChunkCount {
						Or += "uid IN (" + strings.Join(val, ",") + ") OR "
					} else {
						Or += "uid IN (" + strings.Join(val, ",") + ")"
					}

				}
				Or += " ) "
				cond_str := Or
				cond_str += " AND payout_category = '" + payout_category + "'"
				cond_str += " AND wallet_category = '" + wallet_category + "'"
				master_user_balance, _ := GoDB.FetchAllOrderGroup(db, masterTb, "uid,SUM(balance) as total_vol", "", cond_str, "", "", "", "uid,payout_category")

				if len(master_user_balance) > 0 {
					for _, item := range master_user_balance {
						uid__data_map[item["uid"]] = item["total_vol"]
					}
				}

				for _, UidD := range existing_uids_unique {
					for _, data := range fields {
						if data["uid"] == UidD {
							amountF, _ = strconv.ParseFloat(data["balance"], 64)
						}
					}
					Previous_amtF, _ := strconv.ParseFloat(uid__data_map[UidD], 64)
					total_sum = Previous_amtF + amountF

					total_vol_fields[fmt.Sprintf("( uid = %v AND payout_category = '%s' )", UidD, payout_category)] = fmt.Sprintf("%v", total_sum)
					MasterUpdateCondArr += fmt.Sprintf("( uid = %v AND payout_category = '%s' ) OR ", UidD, payout_category)
				}
				MasterUpdateCondArr = strings.TrimRight(MasterUpdateCondArr, "OR ")
				total_vol_arr["fields"] = total_vol_fields
				MASTERUPDATEARRAy["balance"] = total_vol_arr

				MUresponse, MUerr := GoDB.GoMultipleRawUpdate(db, masterTb, MASTERUPDATEARRAy, MasterUpdateCondArr)
				fmt.Println(masterTb, " Bulk Master Updation Now -- ", MUresponse, "errrr", MUerr)
			}
			if len(non_existing_uids_unique) > 0 {
				var fieldArr map[int]map[string]string
				fieldArr = make(map[int]map[string]string, 0)
				var i int
				var amountStr string
				for _, NonUidD := range non_existing_uids_unique {
					// var amount float64
					// amount = 0.0
					for _, data := range fields {
						if data["uid"] == NonUidD {
							amountStr = data["balance"]
							// amountF, _ := strconv.ParseFloat(data["balance"], 64)
							// amount = amount + amountF
							// PR("insert data", data)
							// PR("insert amount", amount)
							// PR(amount)
						}
					}
					if amountStr != "" {
						// amountStr := strconv.FormatFloat(amount, 'f', -1, 64)
						tmp := make(map[string]string, 11)
						tmp["uid"] = NonUidD
						tmp["payout_category"] = payout_category
						tmp["wallet_category"] = wallet_category
						tmp["balance"] = amountStr
						tmp["created"] = afl_date_str
						tmp["updated_on"] = afl_date_str
						tmp["updated_day"] = d
						tmp["updated_month"] = m
						tmp["updated_year"] = y
						tmp["updated_week"] = w
						tmp["updated_date"] = y + "-" + m + "-" + d
						fieldArr[i] = tmp
						i++
					}
				}
				if len(fieldArr) > 0 {
					_, err := GoDB.GoMultiInsert(db, masterTb, fieldArr)
					PR("MasterTransactionMultiUpdate", err)
				}
			}
		}
	}
	return err
}
func AflMasterWalletSplitChannelProcess(db *sql.DB, wg *sync.WaitGroup, ch *chan string, fields map[int]map[string]string, payout_category string, wallet_category string) {
	defer wg.Done()
	var err error
	if len(fields) > 0 {
		walletTb := "afl_multi_wallet_" + wallet_category
		_, err = GoDB.GoMultiInsert(db, walletTb, fields)
		// err = MasterWalletTransactionMultiUpdate(db, Insert_fields, "1", false)
		err = MasterWalletTransactionMultiUpdate_with_Category(db, fields, payout_category, wallet_category, false)
	}
	*ch <- fmt.Sprintf("errors: %d", err)
	log.Println("error message ", err)

}

func TanxBlockAdd(db *sql.DB,name string,unic string){

	fields := map[string]string{
		"name" : name,
		"unc_key" : unic,
	}

	_, _ = GoDB.GoInsert(db, "afl_transaction_blocks", fields)
}

func TanxBlockCheck(db *sql.DB,name string,unic string)(status bool){

	cond := " name = '" + name + "'"
	if unic != ""{
		cond += "AND unc_key = '" + unic + "'"
	}
	
	exist, _ := GoDB.GoRowCount(db, "afl_transaction_blocks", cond)

	if exist > 0 {
		return true
	}else{
		return false
	}

}

func TanxBlockDelete(db *sql.DB,unic string){

	cond := " unc_key = '" + unic + "'"
	GoDB.GoDelete(db, "afl_transaction_blocks", cond)
}
