package agro_engine

import (
  "agro/proto"
  //"os/exec"
  "bytes"
  "agro/db"
  "log"
  "fmt"
  "github.com/fsouza/go-dockerclient"

)


func RunJob(job *agro_pb.Job, workdir string, dbi agro_db.AgroDB) error {
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
  client, err := docker.NewClientFromEnv()
  if err != nil {
    log.Printf("Docker Error\n")
    return fmt.Errorf("Docker Error")
  }
  //fmt.Printf("%s\n", client)
  /*
  images, err := client.ListImages(docker.ListImagesOptions{All:false})
  for _, i := range images {
    log.Printf("%s\n", i.RepoTags)
  }
  */
  
  container, err := client.CreateContainer(docker.CreateContainerOptions{
    Config: &docker.Config{
      Image:*job.Container,
      Cmd:args,
    },
  })  
  if err != nil {
    log.Printf("Docker run Error: %s", err)
    return err
  }
  
  var stdout,stderr bytes.Buffer
  err = client.StartContainer(container.ID, &docker.HostConfig{})
  if err != nil {
    log.Printf("Docker run Error: %s", err)
    return err    
  }
  client.AttachToContainer(docker.AttachToContainerOptions{
    Container:container.ID,
    OutputStream:&stdout,
    ErrorStream:&stderr,
    Logs:true,
    Stream:true,
    Stdout:true,
    Stderr:true,
  })
  /*
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
  */
  dbi.SetJobLogs(*job.ID, stdout.Bytes(), stderr.Bytes())
  return err
}