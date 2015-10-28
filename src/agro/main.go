package main


import (
    "log"
    "net/http"
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


type AgroServer struct {
  dbi agro_db.AgroDB
  engine agro_engine.WorkEngine
}

func (self *AgroServer) MainPage(w http.ResponseWriter, r *http.Request) {
    w.Write([]byte("Main Page\n"))
}

func (self AgroServer) AddTask(ctx context.Context, task *agro_pb.Task) (*agro_pb.TaskStatus, error) {
  log.Printf("Received: %s\n", task)
  
  err := self.dbi.AddTask(task)
  if err != nil {
    log.Printf("%s\n", err)
  }
  state := agro_pb.State_QUEUED
  return &agro_pb.TaskStatus{
      ID : task.ID,
      State : &state,
  }, nil
}

func (self AgroServer) AddWorkflow(ctx context.Context, workflow *agro_pb.Workflow) (*agro_pb.WorkflowStatus, error) {
  return nil, nil
}

func (self AgroServer) GetTaskStatus(ctx context.Context, in *agro_pb.IDQuery) (*agro_pb.TaskStatus, error) {
  return nil, nil
}

func (self AgroServer) GetJobStatus(ctx context.Context, in *agro_pb.IDQuery) (*agro_pb.JobStatus, error) {
  return nil, nil
}


func main() {
    mesos_master := flag.String("mesos", "localhost:5050", "help message for flagname")

    flag.Parse()
    log.Println("Starting DB Connection")
    dbi, _ := agro_db.NewMongo("localhost")
    manager, _ := agro_mesos.NewMesosManager()
    engine, _ := agro_engine.NewEngine(dbi, manager, 4)
    server := AgroServer {
      dbi : dbi,
      engine: engine,
    }
    manager.Run(*mesos_master)
    
    lis, err := net.Listen("tcp", ":9713")
    if err != nil {
      panic("Cannot open port")
    }
    grpcServer := grpc.NewServer()
  	agro_pb.RegisterTasksServer(grpcServer, server)
    log.Println("Starting Server")
  	grpcServer.Serve(lis)
    
    /*
    r := mux.NewRouter()
    // Routes consist of a path and a handler function.
    r.HandleFunc("/", server.MainPage)
    r.HandleFunc("/api/task", server.AddTask).Methods("POST")
    
    // Bind to a port and pass our router in
    http.ListenAndServe(":8000", r)
    */
}