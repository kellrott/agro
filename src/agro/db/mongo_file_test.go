package agro_db


import (
  "fmt"
  "testing"
  "agro/proto"
  uuid "github.com/nu7hatch/gouuid"
  proto "github.com/golang/protobuf/proto"
  "os"
  "io/ioutil"
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
    UUID: proto.String(file_id),
  }
  w, _ := os.Getwd()
  fmt.Printf("Dis: %s\n", w)
  data, _ := ioutil.ReadFile("../../../README")
  dbi.CreateFile(finfo)
  
  fmt.Printf("Sending: %s\n", data)
  
  packet := agro_pb.DataBlock {
    UUID: proto.String(file_id),
    Start: proto.Int64(0),
    Len: proto.Int64(int64(len(data))),
    Data: data,
  }
  dbi.WriteFile(packet)
  dbi.CommitFile( agro_pb.FileID{UUID:proto.String(file_id)} )
  
  info := dbi.GetFileInfo( agro_pb.FileID{UUID:proto.String(file_id)} )
  fmt.Printf("FileSize: %d\n", *info.Size)
  
  block := dbi.ReadFile( agro_pb.ReadRequest{
      UUID:  proto.String(file_id),
      Start: proto.Int64(0),
      Size:  info.Size,
  })
  
  fmt.Printf("ReadOut:%d '%s'\n", *block.Len, string(block.Data))  
}
