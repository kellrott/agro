package main


import (
    "log"
    "net/http"
    //"io/ioutil"
    "flag"
    //"github.com/gorilla/mux"
    //"github.com/golang/protobuf/jsonpb"
    "agro/db"
    "agro/proto"
    "agro/engine"
    "agro/engine/drmaa"
    context "golang.org/x/net/context"
)



func main() {
    flag.Parse()
    log.Println("Starting DB Connection")
    dbi, _ := agro_db.NewMongo("localhost")
    var manager agro_engine.JobManager = nil
    manager, _ = agro_drmaa.NewDrmaaManager()
    
    engine, _ := agro_engine.NewEngine(dbi, manager, 4)
    server := AgroServer {
      dbi : dbi,
      engine: engine,
    }
    engine.Start()
    lis, err := net.Listen("tcp", ":9713")
    if err != nil {
      panic("Cannot open port")
    }
    grpcServer := grpc.NewServer()
  	agro_pb.RegisterTasksServer(grpcServer, server)
    log.Println("Starting Server")
  	grpcServer.Serve(lis)

}