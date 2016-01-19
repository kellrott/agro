package agro_engine

import (
  "agro/proto"
  "os/exec"
  "path"
  //"bytes"
  "log"
  "fmt"
  "os"
  "strings"
  "github.com/fsouza/go-dockerclient"
  "io/ioutil"
  proto "github.com/golang/protobuf/proto"
  //"path"
  "golang.org/x/net/context"
)

const BLOCK_SIZE = int64(10485760)

const HEADER_SIZE = int64(102400)
func read_file_head(path string) []byte {
  f, _ := os.Open(path)
  buffer := make([]byte, HEADER_SIZE)
  l, _ := f.Read(buffer)
  f.Close()
  return buffer[:l]
}


func download_file(fileID string, filePath string, dbi agro_pb.FileStoreClient) error {
  f, err := os.Create(filePath)
  if err != nil {
    log.Printf("Unable to create workfile")
    return err
  }
  info, err := dbi.GetFileInfo( context.Background(), &agro_pb.FileID{Id: proto.String(fileID) } )
  for i := int64(0); i < *info.Size; i+= BLOCK_SIZE {
    block, _ := dbi.ReadFile(context.Background(), &agro_pb.ReadRequest{
      Id: info.Id,
      Start: &i, 
      Size: proto.Int64(BLOCK_SIZE),
    })
    f.Write(block.Data)
  }
  f.Close()
  return nil
}


func upload_file(fileID string, filePath string, dbi agro_pb.FileStoreClient) error {

  file, _ := os.Open(filePath)
  
  finfo := agro_pb.FileInfo{
    Name: proto.String(path.Base(filePath)),
    Id: proto.String(fileID),
  }
  dbi.CreateFile(context.Background(), &finfo)
  buffer := make([]byte, BLOCK_SIZE)
  bytes_written := int64(0)
  for {
    n, _ := file.Read(buffer)
    if n == 0 { break }
    packet := agro_pb.DataBlock {
      Id: proto.String(fileID),
      Start: proto.Int64(bytes_written),
      Len: proto.Int64(int64(n)),
      Data: buffer[:n],
    }
    dbi.WriteFile(context.Background(), &packet)
    bytes_written += int64(n)
  }
  file.Close()
  dbi.CommitFile(context.Background(), &agro_pb.FileID{Id:proto.String(fileID)} )
  return nil
}


func RunJob(job *agro_pb.Job, workdir string, dbi agro_pb.FileStoreClient) ([]byte, []byte, error) {
  wdir, err := ioutil.TempDir(workdir, "agrojob_")
  if err != nil {
    log.Printf("Unable to create workdir")
    return []byte(""), []byte(""), err
  }
  args := make([]string, 0, len(job.Args) + 1)
  args = append(args, *job.Command)
  arg_files := make(map[int]string)
  for i, arg := range(job.Args) {
    if arg.GetFileArg() != nil {
      var filePath string = ""
      file_arg_type := arg.GetFileArg().GetType() 
      if file_arg_type == agro_pb.FileArgument_NAMED {
        filePath = path.Join(workdir, path.Base(arg.GetFileArg().GetName()))
      } else if (file_arg_type != agro_pb.FileArgument_STDOUT && file_arg_type != agro_pb.FileArgument_STDERR ) {
        f, err := ioutil.TempFile(wdir, "workfile_")
        if err != nil {
          log.Printf("Unable to create workfile")
          return []byte(""), []byte(""), err
        }
        log.Printf("Setting up file: %s", f.Name())
        filePath = f.Name()
        f.Close()
      }
      if arg.GetFileArg().GetInput() {
        download_file(arg.GetFileArg().GetId(), filePath, dbi)
      }
      if !arg.GetFileArg().GetSilent() && file_arg_type != agro_pb.FileArgument_STDOUT && file_arg_type != agro_pb.FileArgument_STDERR {
        if file_arg_type == agro_pb.FileArgument_REGEX {
          p := strings.Replace(arg.GetFileArg().GetName(), arg.GetFileArg().GetNameKey(), filePath, -1)
          args = append(args, p)
        } else {
          args = append(args, filePath)          
        }
      }
      arg_files[i] = filePath
    } else {
      args = append(args, arg.GetArg())        
    }
  }
  
  stdout, err := ioutil.TempFile(wdir, "stdout_")
  stderr, err := ioutil.TempFile(wdir, "stderr_")
  stdout_path := stdout.Name()
  stderr_path := stderr.Name()

  if job.Container != nil {
    client, err := docker.NewClientFromEnv()
    if err != nil {
      log.Printf("Docker Error\n")
      return []byte(""), []byte(""), fmt.Errorf("Docker Error")
    }
    create_config := docker.Config{
      Image:*job.Container,
      Cmd:args,
      AttachStderr:true,
      AttachStdout:true,
    }
    binds := []string{
      fmt.Sprintf("%s:%s", wdir, wdir),
    }
    for _, req := range job.Requirements {
      if *req.Name == "docker_socket" {
        //TODO: make sure this is allowed
        binds = append(binds, "/var/run/docker.sock:/var/run/docker.sock")
      }
    }
    log.Printf("Starting Docker: %#v", create_config)
    container, err := client.CreateContainer(docker.CreateContainerOptions{
      Config: &create_config,
    })  
    if err != nil {
      log.Printf("Docker run Error: %s", err)
      return []byte(""), []byte(""), err
    }
    



    log.Printf("Starting Docker: %s", strings.Join(args, " "))
    err = client.StartContainer(container.ID, &docker.HostConfig {
  		Binds: binds,
  	})
    if err != nil {
      log.Printf("Docker run Error: %s", err)
      return []byte(""), []byte(""), err
    }
    log.Printf("Attaching Container: %s", container.ID)
    /*
    err = client.AttachToContainer(docker.AttachToContainerOptions{
      Container:container.ID,
      OutputStream:stdout,
      ErrorStream:stderr,
      Logs:true,
      Stream:true,
      Stdout:true,
      Stderr:true,
    })
    */
    client.WaitContainer(container.ID)
    client.Logs(docker.LogsOptions{Container:container.ID, Stderr:true, Stdout:true, OutputStream:stdout, ErrorStream:stderr})
    if err != nil {
      log.Printf("docker %s error: %s", container.ID, err)
    } else {
      log.Printf("docker %s complete", container.ID, err)
    }
    client.RemoveContainer(docker.RemoveContainerOptions{ID:container.ID,RemoveVolumes:true})
  } else {
    cmd := exec.Cmd{
      Path:*job.Command,
      Args:args,
      Dir: "",
    }
    cmd.Stdout = stdout
    cmd.Stderr = stderr  
    cmd.Run()
    cmd.Wait()
  }
  stdout.Close()
  stderr.Close()
  
  for i, arg := range(job.Args) {
    if arg.GetFileArg() != nil {
      if !arg.GetFileArg().GetInput() {
        file_id := arg.GetFileArg().Id
        if arg.GetFileArg().GetType() == agro_pb.FileArgument_STDOUT {
          upload_file(*file_id, stdout_path, dbi)
        } else if arg.GetFileArg().GetType() == agro_pb.FileArgument_STDERR {
          upload_file(*file_id, stderr_path, dbi)
        } else {
          upload_file(*file_id, arg_files[i], dbi)
       }
      }
    }
  }

  stderr_text := read_file_head(stderr_path)
  stdout_text := read_file_head(stdout_path)
  return stdout_text, stderr_text, err
}