/**
 * -------------------------------------------------------------------------------
 * @author Dileep
 * Copyright 2018 The Epixelsolutions.pvt.ltd. All rights reserved.
 *
 * Log Error and rollback the logs
 * -------------------------------------------------------------------------------
 */
package PanicError

import "runtime"
import "fmt"
import "ErrorHandler/EpsLogs"
import "database/sql"







/*
 * -------------------------------------------------------------------------------
 * Log the GOLANG erros
 * -------------------------------------------------------------------------------
*/

 func Error_log(db *sql.DB,err error) {
   
    if err != nil {
      pc, fn, line, _ := runtime.Caller(1)
      data := fmt.Sprintf("In %s -%s:%d <b>%v</b>", runtime.FuncForPC(pc).Name(), fn, line, err)
      EpsLogs.Log(db,0,"Panic", data, "", 2)
      fmt.Println(data)
    }
}


func Error_logTx(tx *sql.Tx,err error) {
   
    if err != nil {
      pc, fn, line, _ := runtime.Caller(1)
      data := fmt.Sprintf("In %s -%s:%d <b>%v</b>", runtime.FuncForPC(pc).Name(), fn, line, err)
      EpsLogs.LogTx(tx,0,"Panic", data, "", 2)
      fmt.Println(data)
    }
}






