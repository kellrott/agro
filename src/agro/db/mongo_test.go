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

func doProtoConvert_task(a proto.Message) {
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
    Id : &id,
    Command : &cmd,
    Container: &container,
    State: &state,
    Args : []*agro_pb.CmdArgument{&agro_pb.CmdArgument{Value:&agro_pb.CmdArgument_Arg{"hello"}}},
  }
  doProtoConvert_task(a)
}


func TestProtoConvert_2(t *testing.T) {
  a := &agro_pb.Task{
    Id:proto.String("test"),
    Command:proto.String("/bin/echo Testing"),
    Container:proto.String("ubuntu"),
  }
  doProtoConvert_task(a)
}

func TestProtoConvert_3(t *testing.T) {
  a := map[string]interface{}{
    "_id" : "test",
    "State" : 5,
  }
  d := agro_pb.Job{}
  MongoToProto(a, &d, true)
  dump("Json", a)
  dump("Copy", d)
}



func TestProtoConvert_4(t *testing.T) {
  a := map[string]interface{}{
    "_id" : "test",
    "State" : 5,
    "Info" : map[string]interface{}{
      "ID" : "test_task",
      "State" : 0,
    },
  }
  d := agro_pb.Job{}
  MongoToProto(a, &d, true)
  dump("Json", a)
  dump("Copy", d)
}