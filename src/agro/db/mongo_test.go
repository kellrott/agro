package agro_db


import (
  "agro/proto"
  "fmt"
  "testing"
  "encoding/json"
  proto "github.com/golang/protobuf/proto"

)

func dump(msg string, a interface{}) {
  a_text, _ := json.Marshal(a)
  fmt.Printf("%s\t%s\n", msg, a_text)
}

func doProtoConvert(a proto.Message) {
  b := ProtoToMongo(a, true)
  b_str, _ := json.Marshal(b)
  c := make(map[string]interface{})
  json.Unmarshal(b_str, &c)
  d := agro_pb.Task{}
  MongoToProto(c, &d, true)
  
  dump("Orig", a)
  dump("Json", b)
  dump("Copy", d)
}


func TestProtoConvert_1(t *testing.T) {
  
  id := "Testing"
  cmd := "/bin/echo Hello World"
  state := agro_pb.State_OK
  container := "ubuntu"
  a := &agro_pb.Task{
    ID : &id,
    DataDepends : []string{ "1234", "6789"},
    CommandLine : &cmd,
    Container: &container,
    Status: &agro_pb.TaskStatus {
      ID: &id,
      State: &state,
      Runs:[]*agro_pb.JobStatus{ &agro_pb.JobStatus{ ID:&id } },
    },
  }
  doProtoConvert(a)
}


func TestProtoConvert_2(t *testing.T) {
  a := &agro_pb.Task{
    ID:proto.String("test"),
    CommandLine:proto.String("/bin/echo Testing"),
    Container:proto.String("ubuntu"),
  }
  doProtoConvert(a)
}