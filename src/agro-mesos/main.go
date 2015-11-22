package main


import (
    "log"
    //"io/ioutil"
    "net"
    "flag"
    //"github.com/gorilla/mux"
    //"github.com/golang/protobuf/jsonpb"
    "google.golang.org/grpc"
    "agro/db"
    "agro/proto"
    "agro/engine"
    "agro/engine/mesos"
    context "golang.org/x/net/context"
)


func main() {
    mongo := flag.String("mongo", "localhost", "Mongo Server")
    mesos_master := flag.String("mesos", "localhost:5050", "Mesos Server")

    flag.Parse()
    log.Println("Starting DB Connection")
    dbi, _ := agro_db.NewMongo(*mongo)
    var manager agro_engine.JobManager = nil
    manager, _ = agro_mesos.NewMesosManager(*mesos_master)
    
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
  	agro_pb.RegisterAgroServer(grpcServer, server)
    log.Println("Starting Server")
  	grpcServer.Serve(lis)
    
}