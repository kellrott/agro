package agro_engine

import (
  "agro/proto"
  "os/exec"
  "bytes"
  "agro/db"
  "log"
)


func RunJob(job *agro_pb.Job, dbi agro_db.AgroDB) error {
  args := make([]string, 0, len(job.Args) + 1)
  args = append(args, *job.Command)
  for _,i := range(job.Args) {
    if i.GetFile() != nil {
      log.Printf("Setting up file")
      //args = append(args, i.GetFile())
      args = append(args, "test")
    } else {
      args = append(args, i.GetArg())        
    }
  }
  cmd := exec.Cmd{
    Path:*job.Command,
    Args:args,
    Dir: "",
  }
  var stdout,stderr bytes.Buffer
  cmd.Stdout = &stdout
  cmd.Stderr = &stderr  
  cmd.Run()
  err := cmd.Wait()
  dbi.SetJobLogs(*job.ID, stdout.Bytes(), stderr.Bytes())
  return err
}