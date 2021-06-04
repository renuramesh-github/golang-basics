/**
 * --------------------------------------------------
 *  @updated dileep
 *  Extend and modified the error logs
 * --------------------------------------------------
 */

package EpsLogs

import "GoDB"
import "GoTxDB"
import "Settings"
import "fmt"
import "time"
import "net"
import "os"
import "runtime"
import "database/sql"
import "phpserialize"

var GO_WATCHDOG_EMERGENCY = 0 //Log message severity -- Emergency: system is unusable.
var GO_WATCHDOG_ALERT = 1     //Log message severity -- Alert: action must be taken immediately.
var GO_WATCHDOG_CRITICAL = 2  //Log message severity -- Critical conditions.
var GO_WATCHDOG_ERROR = 3     //Log message severity -- Error conditions.
var GO_WATCHDOG_WARNING = 4   //Log message severity -- Warning conditions.
var GO_WATCHDOG_NOTICE = 5    //Log message severity -- Normal but significant conditions.
var GO_WATCHDOG_INFO = 6      //Log message severity -- Informational messages.
var GO_WATCHDOG_DEBUG = 7     //Log message severity -- Debug-level messages.

/*
 * -----------------------------------------------------------------------
 * Save the log messages into db
 * -----------------------------------------------------------------------
 */
func Log(db *sql.DB, uid int, types string, message string, variables string, severity int) {

  unix_time_stamp := system_time(db)

  // host_name := BinaryCommon.GetHostName()
  ip_address := GetSystemIp()
  var m []string
  out, _ := phpserialize.Marshal(m, nil)

  stmt, _ := db.Prepare("INSERT watchdog SET uid=?,type=?,message=?, variables = ?,severity = ?,hostname = ?,timestamp =?,location = ?")

  //Error_log(db,err)

  stmt.Exec(uid, types, message, out, severity, ip_address, unix_time_stamp, "")

  //Error_log(db,err)

}

//for db transaction 
func LogTx(tx *sql.Tx, uid int, types string, message string, variables string, severity int) {

  unix_time_stamp := system_timeTx(tx)

  // host_name := BinaryCommon.GetHostName()
  ip_address := GetSystemIp()
  var m []string
  out, _ := phpserialize.Marshal(m, nil)

  stmt, _ := tx.Prepare("INSERT watchdog SET uid=?,type=?,message=?, variables = ?,severity = ?,hostname = ?,timestamp =?,location = ?")

  //Error_log(db,err)

  stmt.Exec(uid, types, message, out, severity, ip_address, unix_time_stamp, "")

  //Error_log(db,err)

}
func dateFormat(dtstr string) string {
  fmt.Println(dtstr)
  dt, _ := time.Parse(Settings.Site.SitePublished, dtstr)
  return dt.Format(Settings.Site.SitePublished)
}

/*
 * -----------------------------------------------------------------------
 * Get the system time
 * -----------------------------------------------------------------------
 */
func system_time(db *sql.DB) int64 {
  TEXTMODE, _ := GoDB.AFLVariableGet(db, "afl_enable_test_mode")

  TEXTDATE, _ := GoDB.AFLVariableGet(db, "afl_enable_test_date")

  /*
   * -----------------------------------------
   * Get current system date
   * -----------------------------------------
   */
  t := time.Now()
  t.String()
  current_date := (t.Format(Settings.Site.SitePublished))

  if TEXTMODE == "1" && TEXTDATE == "1" {
    current_date, _ = GoDB.AFLVariableGet(db, "afl_testing_date")
    current_date = current_date + ":00"
    current_date = dateFormat(current_date)

  }

  TIMEZONE, _ := GoDB.VariableGet(db, "date_default_timezone")

  // First, we create an instance of a timezone location object
  loc, _ := time.LoadLocation(TIMEZONE)

  // this is our custom format. Note that the format must point to this exact time
  format := Settings.Site.SitePublished

  // this is your timestamp
  timestamp := current_date

  system_time, _ := time.ParseInLocation(format, timestamp, loc)

  unix_time_stamp := system_time.Unix()

  return unix_time_stamp
}


func system_timeTx(tx *sql.Tx) int64 {
  TEXTMODE, _ := GoTxDB.AFLVariableGet(tx, "afl_enable_test_mode")

  TEXTDATE, _ := GoTxDB.AFLVariableGet(tx, "afl_enable_test_date")

  /*
   * -----------------------------------------
   * Get current system date
   * -----------------------------------------
   */
  t := time.Now()
  t.String()
  current_date := (t.Format(Settings.Site.SitePublished))

  if TEXTMODE == "1" && TEXTDATE == "1" {
    current_date, _ = GoTxDB.AFLVariableGet(tx, "afl_testing_date")
    current_date = current_date + ":00"
    current_date = dateFormat(current_date)

  }

  TIMEZONE, _ := GoTxDB.VariableGet(tx, "date_default_timezone")

  // First, we create an instance of a timezone location object
  loc, _ := time.LoadLocation(TIMEZONE)

  // this is our custom format. Note that the format must point to this exact time
  format := Settings.Site.SitePublished

  // this is your timestamp
  timestamp := current_date

  system_time, _ := time.ParseInLocation(format, timestamp, loc)

  unix_time_stamp := system_time.Unix()

  return unix_time_stamp
}
/*
 * -------------------------------------------------------------------------------
 * Get system ip
 * -------------------------------------------------------------------------------
 */
func GetSystemIp() string {
  var value string
  addrs, err := net.InterfaceAddrs()
  if err != nil {
    value = ("Oops: " + err.Error())
  }

  for _, a := range addrs {
    if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
      if ipnet.IP.To4() != nil {
        value = (ipnet.IP.String())
      }
    }
  }

  return value
}

/*
 * -------------------------------------------------------------------------------
 * Get host name
 * -------------------------------------------------------------------------------
 */
func GetHostName() string {
  name, err := os.Hostname()
  if err != nil {
    panic(err)
  }
  return name
}

/*
 * -------------------------------------------------------------------------------
 * Log the GOLANG erros
 * -------------------------------------------------------------------------------
 */
func Error_log(db *sql.DB, err error) {
  if err != nil {
    if err == sql.ErrNoRows {
      //Do nothing
    } else {
      // panic(err)
      pc, fn, line, _ := runtime.Caller(1)
      data := fmt.Sprintf("[error] in %s[%s:%d] %v", runtime.FuncForPC(pc).Name(), fn, line, err)

      Log(db, 0, "Panic", data, "", 1)
    }
  }
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
