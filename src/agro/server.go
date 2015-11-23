
package agro

import (
  "log"
  "net"
  "google.golang.org/grpc"
  "agro/proto"
  "agro/db"
  "agro/engine"
  context "golang.org/x/net/context"
)

type AgroSchedServer struct {
  engine agro_engine.WorkEngine
}

type AgroFileServer struct {
  dbi agro_db.AgroDB
}

type AgroServer struct {
  schedServer *AgroSchedServer
  fileServer  *AgroFileServer
}

func NewAgroServer() *AgroServer {
  return &AgroServer {}
}

func (self *AgroServer) RegisterEngine(engine agro_engine.WorkEngine) {
  self.schedServer = &AgroSchedServer{engine:engine}
}

func (self *AgroServer) RegisterFileStore(dbi agro_db.AgroDB) {
  self.fileServer = &AgroFileServer{dbi:dbi}
}

func (self *AgroServer) Run() {
  lis, err := net.Listen("tcp", ":9713")
  if err != nil {
    panic("Cannot open port")
  }
  grpcServer := grpc.NewServer()
  if self.schedServer != nil {
    agro_pb.RegisterAgroServer(grpcServer, self.schedServer)
  }
  if self.fileServer != nil {
    agro_pb.RegisterFileStoreServer(grpcServer, self.fileServer)
  }
  log.Println("Starting Server")
  grpcServer.Serve(lis)
}

func (self AgroSchedServer) AddTask(ctx context.Context, task *agro_pb.Task) (*agro_pb.TaskStatus, error) {
  log.Printf("Received: %s\n", task)
  a := agro_pb.State_QUEUED
  task.State = &a
  err := self.engine.AddTask(task)
  if err != nil {
    log.Printf("Error: %s\n", err)
  }
  state := agro_pb.State_QUEUED
  return &agro_pb.TaskStatus{
      ID : task.ID,
      State : &state,
  }, nil
}

func (self AgroSchedServer) SearchTasks(tags *agro_pb.TagArray, stream agro_pb.Agro_SearchTasksServer) (error) {
  log.Printf("Server Search Tasks: %s", tags)
  for i := range self.engine.GetDBI().SearchTasks(tags) {
    if err := stream.Send(&i); err != nil {
      return err
    }
  }
  return nil
}

func (self AgroSchedServer) GetTaskStatus(in *agro_pb.IDQuery, stream agro_pb.Agro_GetTaskStatusServer) (error) {
  for _, id := range in.IDs {
    out := self.engine.GetDBI().GetTaskStatus(id)
    if err := stream.Send(&out); err != nil {
      return err
    }
  }
  return nil
  
}

func (self AgroSchedServer) GetJobStatus(in *agro_pb.IDQuery, stream agro_pb.Agro_GetJobStatusServer) (error) {
  for _, id := range in.IDs {
    out := self.engine.GetDBI().GetJob(id)
    o := agro_pb.JobStatus{
      ID:out.ID,
      State:out.State,
    }
    if err := stream.Send(&o); err != nil {
      return err
    }
  }
  return nil
}


func (self AgroFileServer) CreateFile(ctx context.Context, info *agro_pb.FileInfo) (*agro_pb.FileState, error) {
  o := self.dbi.CreateFile(*info)
  return &o, nil
}

func (self AgroFileServer) WriteFile(ctx context.Context, block *agro_pb.DataBlock) (*agro_pb.FileState, error) {
  o := self.dbi.WriteFile(*block)
  return &o, nil
}

func (self AgroFileServer) CommitFile(ctx context.Context, file *agro_pb.FileID) (*agro_pb.FileState, error) {
  o := self.dbi.CommitFile(*file)
  return &o, nil
}

func (self AgroFileServer) GetFileInfo(ctx context.Context, file *agro_pb.FileID) (*agro_pb.FileInfo, error) {
  o := self.dbi.GetFileInfo(*file)
  return &o, nil
}

func (self AgroFileServer) ReadFile(ctx context.Context, req *agro_pb.ReadRequest) (*agro_pb.DataBlock, error) {
  o := self.dbi.ReadFile(*req)
  return &o, nil
}
