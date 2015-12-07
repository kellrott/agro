package main

import (
  "flag"
  "net/http"
  "agro/proto"
  "github.com/gorilla/mux"
  "golang.org/x/net/context"
	"google.golang.org/grpc"
  proto "github.com/golang/protobuf/proto"
)

const BLOCK_SIZE = int64(10485760)

type Webserver struct {
  client agro_pb.SchedulerClient
  dbi    agro_pb.FileStoreClient
}

func (self *Webserver) MainPage(w http.ResponseWriter, r *http.Request) {
  w.Write([]byte("MainPage!\n"))
}


func (self *Webserver) Tasks(w http.ResponseWriter, r *http.Request) {
  task_id := "test"
  self.client.GetTaskStatus(context.Background(), &agro_pb.IDQuery{Ids:[]string{task_id}})
  w.Write([]byte("MainPage!\n"))
}

func (self *Webserver) ListFiles(w http.ResponseWriter, r *http.Request) {
  task_id := "test"
  self.client.GetTaskStatus(context.Background(), &agro_pb.IDQuery{Ids:[]string{task_id}})
  w.Write([]byte("MainPage!\n"))
}

func (self *Webserver) GetFile(w http.ResponseWriter, r *http.Request) {
  params := mux.Vars(r)
  fileID := params["id"]
  
  info, _ := self.dbi.GetFileInfo(context.Background(), &agro_pb.FileID{Id: proto.String(fileID) })
  for i := int64(0); i < *info.Size; i+= BLOCK_SIZE {
    block, _ := self.dbi.ReadFile(context.Background(), 
      &agro_pb.ReadRequest{
        Id: info.Id,
        Start: &i, 
        Size: proto.Int64(BLOCK_SIZE),
    })
    w.Write(block.Data)
  }
}

func main() {
  agro_server := flag.String("agro", "localhost:9713", "Agro Server")
  http_port   := flag.String("port", "8000", "HTTP Port")
  conn, err := grpc.Dial(*agro_server, grpc.WithInsecure())
  if err != nil {
    panic(err)
  }
  defer conn.Close()
  agro_client := agro_pb.NewSchedulerClient(conn)
  file_client := agro_pb.NewFileStoreClient(conn)
  server := &Webserver{agro_client, file_client}
  r := mux.NewRouter()
  // Routes consist of a path and a handler function.
  r.HandleFunc("/", server.MainPage)
  r.HandleFunc("/api/task", server.Tasks)
  r.HandleFunc("/api/file", server.ListFiles)
  r.HandleFunc("/api/file/{id}", server.GetFile)

  // Bind to a port and pass our router in
  http.ListenAndServe(":" + *http_port, r)  

}