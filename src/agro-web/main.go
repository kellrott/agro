package main

import (
  "flag"
  "net/http"
  "agro/proto"
  "github.com/gorilla/mux"
  "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Webserver struct {
  client agro_pb.TasksClient
}

func (self *Webserver) MainPage(w http.ResponseWriter, r *http.Request) {
  w.Write([]byte("MainPage!\n"))
}


func (self *Webserver) Tasks(w http.ResponseWriter, r *http.Request) {
  task_id := "test"
  self.client.GetTaskStatus(context.Background(), &agro_pb.IDQuery{ID:&task_id})
  w.Write([]byte("MainPage!\n"))
}

func main() {
  agro_server := flag.String("agro", "localhost:9713", "Agro Server")
  
  conn, err := grpc.Dial(*agro_server, grpc.WithInsecure())
  if err != nil {
    panic(err)
  }
  defer conn.Close()
  client := agro_pb.NewTasksClient(conn)
  
  server := &Webserver{client}
  r := mux.NewRouter()
  // Routes consist of a path and a handler function.
  r.HandleFunc("/", server.MainPage)
  r.HandleFunc("/api/task", server.Tasks)

  // Bind to a port and pass our router in
  http.ListenAndServe(":8000", r)  

}