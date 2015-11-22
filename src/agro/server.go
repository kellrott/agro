
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

type AgroServer struct {
  dbi agro_db.AgroDB
  engine agro_engine.WorkEngine
}

func NewAgroServer(dbi agro_db.AgroDB, engine agro_engine.WorkEngine) AgroServer {
  return AgroServer{
    dbi:dbi,
    engine:engine,
  }
}

func (self AgroServer) Run() {
  lis, err := net.Listen("tcp", ":9713")
  if err != nil {
    panic("Cannot open port")
  }
  grpcServer := grpc.NewServer()
  agro_pb.RegisterAgroServer(grpcServer, self)
  log.Println("Starting Server")
  grpcServer.Serve(lis)
}

func (self AgroServer) AddTask(ctx context.Context, task *agro_pb.Task) (*agro_pb.TaskStatus, error) {
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

func (self AgroServer) SearchTasks(tags *agro_pb.TagArray, stream agro_pb.Agro_SearchTasksServer) (error) {
  log.Printf("Server Search Tasks: %s", tags)
  for i := range self.dbi.SearchTasks(tags) {
    if err := stream.Send(&i); err != nil {
      return err
    }
  }
  return nil
}

func (self AgroServer) GetTaskStatus(in *agro_pb.IDQuery, stream agro_pb.Agro_GetTaskStatusServer) (error) {
  for _, id := range in.IDs {
    out := self.dbi.GetTaskStatus(id)
    if err := stream.Send(&out); err != nil {
      return err
    }
  }
  return nil
  
}

func (self AgroServer) GetJobStatus(in *agro_pb.IDQuery, stream agro_pb.Agro_GetJobStatusServer) (error) {
  for _, id := range in.IDs {
    out := self.dbi.GetJob(id)
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

