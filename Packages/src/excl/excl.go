/**
 * ------------------------------------------------------
 *  excl packs your Go application(s) for mutual exclution
 *  @author Dileep <dileep@epixelsolutions.com>
 * ------------------------------------------------------
 */

package excl

import (
         "fmt"
         "os"
         "strconv"
			   "io/ioutil"
			   "os/exec"
			   "bytes"
			   "strings"	  
			   "errors"   
 )




// Pipeline strings together the given exec.Cmd commands in a similar fashion
// to the Unix pipeline.  Each command's standard output is connected to the
// standard input of the next command, and the output of the final command in
// the pipeline is returned, along with the collected standard error of all
// commands and the first error found (if any).
//
// To provide input to the pipeline, assign an io.Reader to the first's Stdin.
func Pipeline(cmds ...*exec.Cmd) (pipeLineOutput, collectedStandardError []byte, pipeLineError error) {
        // Require at least one command
        if len(cmds) < 1 { 
                return nil, nil, nil
        }

        // Collect the output from the command(s)
        var output bytes.Buffer
        var stderr bytes.Buffer

        last := len(cmds) - 1
        for i, cmd := range cmds[:last] {
                var err error
                // Connect each command's stdin to the previous command's stdout
                if cmds[i+1].Stdin, err = cmd.StdoutPipe(); err != nil {
                        return nil, nil, err
                }
                // Connect each command's stderr to a buffer
                cmd.Stderr = &stderr
        }

        // Connect the output and error for the last command
        cmds[last].Stdout, cmds[last].Stderr = &output, &stderr

        // Start each command
        for _, cmd := range cmds {
                if err := cmd.Start(); err != nil {
                        return output.Bytes(), stderr.Bytes(), err
                }
        }

        // Wait for each command to complete
        for _, cmd := range cmds {
                if err := cmd.Wait(); err != nil {
                        return output.Bytes(), stderr.Bytes(), err
                }
        }

        // Return the pipeline output and the collected standard error
        return output.Bytes(), stderr.Bytes(), nil
}

/*
 * ExecShellCommand is used to find the number active process count
 */
func ExecShellCommand(dat []byte) (output1 []byte, pipeLineError error){
	 cmd1 := exec.Command("ps", "-ef")
   cmd2 := exec.Command("grep",string(dat))
   cmd3 := exec.Command("grep","-v","'grep'")
   cmd4 := exec.Command("wc","-l")
   output,_, err := Pipeline(cmd1, cmd2, cmd3,cmd4)
   return output,err;
}

func CheckProcessExist(filename string) (flag bool,err error){

	 if _, err := os.Stat(filename); os.IsNotExist(err) {
        file, err := os.Create(filename)
				if err != nil { return false,err }
			  defer file.Close()
   } 

   //Get the process id from the cron.pid
   dat, err := ioutil.ReadFile(filename)
   if err != nil {
   	  return false,err
   }
   //check the process is already running or not
   if string(dat) != "" {
	   output,err := ExecShellCommand(dat)
	   if err != nil {
	   	  return false,err
	   }
	    //if yes panic the error
		 s := string(output);
		 t := strings.TrimSpace(s)
		 
		 i, err := strconv.Atoi(t)
		 fmt.Println(t);
		 if err != nil {
		   	return false,err
		  }


		  if i > 1 {
			 	return false,errors.New("error: previous cron (pid: "+string(dat)+") still running! quitting early") 
			}
	 }
	  //Else write process id in the file and process the cron
	  pid := os.Getpid()
    fmt.Println("Own process identifier: ", strconv.Itoa(pid))

    var d1 = []byte(strconv.Itoa(pid))
    var Werr = ioutil.WriteFile(filename, d1, 0644)
    if Werr != nil {
   	  return false,Werr
   }
    return true,nil
}


func ProcessWatcher(filename string) (bool){

   if _, err := os.Stat(filename); os.IsNotExist(err) {
        file, err := os.Create(filename)
        if err != nil { return false }
        defer file.Close()
   } 

   //Get the process id from the cron.pid
   dat, err := ioutil.ReadFile(filename)
   if err != nil {
      return false
   }
   //check the process is already running or not
   if string(dat) != "" {
     output,err := ExecShellCommand(dat)
     if err != nil {
        return false
     }
      //if yes panic the error
     s := string(output);
     t := strings.TrimSpace(s)
     
     i, err := strconv.Atoi(t)
    
     if err != nil {
        return false
      }


      if i > 1 {
        return true
      }
   }
 
    return false
}


