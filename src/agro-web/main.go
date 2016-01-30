package main

import (
	"io"
	"os"
	"path/filepath"
	"flag"
	"net/http"
	"agro/proto"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	proto "github.com/golang/protobuf/proto"
	"fmt"
	"agro"
	"encoding/json"
)

const BLOCK_SIZE = int64(10485760)

type Webserver struct {
	content_dir string
	client      agro_pb.SchedulerClient
	dbi         agro_pb.FileStoreClient
}

func (self *Webserver) MainPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(self.content_dir, "index.html"))
}

type ClientStreamClient interface {
	Recv() (*proto.Message, error)
}

func stream2array(w io.Writer, results chan map[string]interface{} ) {
	first := true
	w.Write([]byte("["))
	for data := range results {
		if first {
			first = false
		} else {
			w.Write([]byte(","))
		}
		t, _ := json.Marshal(data)
		w.Write(t)
	}
	w.Write([]byte("]"))
}

func (self *Webserver) Tasks(w http.ResponseWriter, r *http.Request) {
	results, _ := self.client.SearchTasks(context.Background(), &agro_pb.TagArray{})
	out := make(chan map[string]interface{})
	go func() {
		for done := false; !done; {
			task, err := results.Recv()
			if err == io.EOF {
				done = true
			} else {
				m := agro.ProtoToMap(task, false)
				out <- m
			}
		}
		close(out)
	}()
	stream2array(w, out)
}

func (self *Webserver) GetTask(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	taskId := params["id"]
	task, _ := self.client.GetTask(context.Background(), &agro_pb.IDQuery{Ids:[]string{taskId}})
	data := agro.ProtoToMap(task, false)

	status_client, _ := self.client.GetTaskStatus(context.Background(), &agro_pb.IDQuery{Ids:[]string{taskId}})
	status, _ := status_client.Recv()
	status_data := agro.ProtoToMap(status, false)
	data["status"] = status_data

	t, _ := json.Marshal(data)
	w.Write(t)
}

func (self *Webserver) GetJob(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	jobId := params["id"]
	job, _ := self.client.GetJob(context.Background(), &agro_pb.IDQuery{Ids:[]string{jobId}} )
	data := agro.ProtoToMap(job, false)
	t, _ := json.Marshal(data)
	w.Write(t)
}

func (self *Webserver) Workers(w http.ResponseWriter, r *http.Request) {
	results, _ := self.client.SearchWorkers(context.Background(), &agro_pb.TagArray{})
	out := make(chan map[string]interface{})
	go func() {
		for done := false; !done; {
			task, err := results.Recv()
			if err == io.EOF {
				done = true
			} else {
				m := agro.ProtoToMap(task, false)
				out <- m
			}
		}
		close(out)
	}()
	stream2array(w, out)
}

func (self *Webserver) ListFiles(w http.ResponseWriter, r *http.Request) {
	//task_id := "test"
	//self.dbi.
	//self.client.GetTaskStatus(context.Background(), &agro_pb.IDQuery{Ids:[]string{task_id}})
	w.Write([]byte("MainPage!\n"))
}

func (self *Webserver) GetFile(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	fileID := params["id"]

	info, _ := self.dbi.GetFileInfo(context.Background(), &agro_pb.FileID{Id: proto.String(fileID) })
	for i := int64(0); i < *info.Size; i += BLOCK_SIZE {
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

	dir, _ := filepath.Abs(os.Args[0])
	dir = filepath.Join(dir, "..", "..", "share")
	agro_server := flag.String("agro", "localhost:9713", "Agro Server")
	http_port := flag.String("port", "8000", "HTTP Port")
	flag.Parse()

	fmt.Printf("Share Dir: %s\n", dir)
	fmt.Printf("Listening on port: %s\n", *http_port)
	conn, err := grpc.Dial(*agro_server, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	agro_client := agro_pb.NewSchedulerClient(conn)
	file_client := agro_pb.NewFileStoreClient(conn)
	server := &Webserver{dir, agro_client, file_client}
	r := mux.NewRouter()
	// Routes consist of a path and a handler function.
	r.HandleFunc("/", server.MainPage)
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir(dir))))
	r.HandleFunc("/api/task", server.Tasks)
	r.HandleFunc("/api/task/{id}", server.GetTask)
	r.HandleFunc("/api/job/{id}", server.GetJob)
	r.HandleFunc("/api/worker", server.Workers)
	r.HandleFunc("/api/file", server.ListFiles)
	r.HandleFunc("/api/file/{id}", server.GetFile)

	// Bind to a port and pass our router in
	http.ListenAndServe(":" + *http_port, r)

}