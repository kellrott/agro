package agro_db


import (
  "fmt"
  "testing"
  "agro/proto"
  uuid "github.com/nu7hatch/gouuid"
  proto "github.com/golang/protobuf/proto"
  "os"
  "io/ioutil"
  "github.com/mesos/mesos-go/examples/Godeps/_workspace/src/golang.org/x/net/context"
)


func TestFileTest(t *testing.T) {
  
  dbi, err := NewMongo("localhost")
  if err != nil {
    panic(err)
  }
  fmt.Printf("Testing")
  
  u, _ := uuid.NewV4()
  file_id := u.String()
  
  finfo := agro_pb.FileInfo{
    Name: proto.String("README"),
    Id: proto.String(file_id),
  }
  w, _ := os.Getwd()
  fmt.Printf("Dis: %s\n", w)
  data, _ := ioutil.ReadFile("../../../README")
  dbi.CreateFile(context.Background(), &finfo)
  
  fmt.Printf("Sending: %s\n", data)
  
  packet := agro_pb.DataBlock {
    Id: proto.String(file_id),
    Start: proto.Int64(0),
    Len: proto.Int64(int64(len(data))),
    Data: data,
  }
  dbi.WriteFile(context.Background(), &packet)
  dbi.CommitFile(context.Background(), &agro_pb.FileID{Id:proto.String(file_id)} )
  
  info, _ := dbi.GetFileInfo(context.Background(), &agro_pb.FileID{Id:proto.String(file_id)} )
  fmt.Printf("FileSize: %d\n", *info.Size)
  
  block, _ := dbi.ReadFile(context.Background(), &agro_pb.ReadRequest{
      Id:  proto.String(file_id),
      Start: proto.Int64(0),
      Size:  info.Size,
  })
  
  fmt.Printf("ReadOut:%d '%s'\n", *block.Len, string(block.Data))  
}
