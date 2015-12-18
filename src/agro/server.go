
package agro

import (
  "log"
  "net"
  "google.golang.org/grpc"
  "agro/proto"
)

type AgroServer struct {
  sched agro_pb.SchedulerServer
  file  agro_pb.FileStoreServer
}

func NewAgroServer() *AgroServer {
  return &AgroServer {}
}

func (self *AgroServer) RegisterScheduler(sched agro_pb.SchedulerServer) {
  self.sched = sched
}

func (self *AgroServer) RegisterFileStore(file agro_pb.FileStoreServer) {
  self.file = file
}

func (self *AgroServer) Run() {
  lis, err := net.Listen("tcp", ":9713")
  if err != nil {
    panic("Cannot open port")
  }
  grpcServer := grpc.NewServer()
  if self.sched != nil {
    agro_pb.RegisterSchedulerServer(grpcServer, self.sched)
  }
  if self.file != nil {
    agro_pb.RegisterFileStoreServer(grpcServer, self.file)
  }
  log.Println("Starting Server")
  grpcServer.Serve(lis)
}
