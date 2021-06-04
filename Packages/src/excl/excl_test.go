package excl

import (
         "testing"
 )

func TestCheckProcessExist(t *testing.T) {
  tables := []struct {
    x string
    n bool
    err error
  }{
    {"cron1.pid", true, nil},
  }

  for _, table := range tables {
    output,_ := CheckProcessExist(table.x)
    if output != table.n  {
      t.Errorf("File of (%s) was incorrect. output (%t)", table.x,output)
      
    }
  }
}