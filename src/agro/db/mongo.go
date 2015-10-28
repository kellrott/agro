
package agro_db


import (
  "log"
  "fmt"
  "strings"
  "gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
  "agro/proto"
  "reflect"
  proto "github.com/golang/protobuf/proto"
)


type MongoInterface struct {
  url string
  session *mgo.Session
  db *mgo.Database
}

func ProtoToMongo(pb proto.Message, idFix bool) map[string]interface{} {
  ref := reflect.ValueOf(pb)
  out := make(map[string]interface{})
  s := ref.Elem()
  for i := 0; i < s.NumField(); i++ {
    value := s.Field(i)
    valueField := s.Type().Field(i)
    if !strings.HasPrefix(valueField.Name, "XXX_") {
      if value.Kind() == reflect.Struct {
        out[valueField.Name] = ProtoToMongo(value.Addr().Interface().(proto.Message), false)
      } else {
        if idFix && valueField.Name == "ID" {
          out["_id"] = value.Interface()
        } else {
          out[valueField.Name] = value.Interface()
        }
      }
    }
  }
  return out
}

func MongoToProto(in interface{}, pb proto.Message, idFix bool) {
  elm := reflect.ValueOf(pb).Elem()
  elmType := elm.Type()
  for i := 0; i < elm.NumField(); i++ {
    f := elm.Field(i)
    ft := elmType.Field(i)
    if strings.HasPrefix(ft.Name, "XXX_") {
      continue
    }
    name := ft.Name
    if idFix && name == "ID"{
      name = "_id"
    }
    if inVal, ok := in.(map[string]interface{})[name]; ok {
      if f.Kind() == reflect.Struct { 
        MongoToProto(inVal, f.Addr().Interface().(proto.Message), false)
      } else {
        mongoToValue(inVal, f )
        //f.Set( reflect.ValueOf(inVal) ) 
      }
    }
  }  
}

func mongoToValue(in interface{}, val reflect.Value) {
  if val.Type().Kind() == reflect.Ptr && in != nil {
    val.Set(reflect.New(val.Type().Elem()))
		mongoToValue(in, val.Elem())
    return
  }
  if val.Type().Kind() == reflect.String {
    val.SetString(in.(string))
    return
  }
  if val.Type().Kind() == reflect.Struct && in != nil{
    MongoToProto(in, val.Addr().Interface().(proto.Message), false)
    return
  }
  if val.Type().Kind() == reflect.Slice && in != nil {
    slc := in.([]interface{})
    len := len(slc)
    val.Set(reflect.MakeSlice(val.Type(), len, len))
		for i := 0; i < len; i++ {
			mongoToValue(slc[i], val.Index(i))
    }
    return
  }
  if val.Type().Kind() == reflect.Uint64 || val.Type().Kind() == reflect.Int64 {
    val.SetInt(in.(int64))
  }
  if val.Type().Kind() == reflect.Uint32 || val.Type().Kind() == reflect.Int32 {
    val.SetInt(int64(in.(float64)))
  }
  fmt.Println(val.Type().Kind())
}

func NewMongo(url string) (*MongoInterface, error) {
  session, err := mgo.Dial(url)
  if err != nil {
    return nil, err
  }
  
  return &MongoInterface{
    url:url,
    session:session,
    db:session.DB("agro"),
  }, nil
}

func (self *MongoInterface) AddTask(task *agro_pb.Task) error {
  e := ProtoToMongo(task, true)
  return self.db.C("task").Insert(e)
}


func (self *MongoInterface) TaskQuery(state *agro_pb.State) chan agro_pb.Task {
  if *state == agro_pb.State_QUEUED {
    log.Printf("Scanning For QUEUED tasks")
    out := make(chan agro_pb.Task)
    go func() {
      i := self.db.C("task").Find( bson.M{"TaskStatus" : nil} ).Iter()
      result := make(map[string]interface{})
      for i.Next(&result) {
        pout := agro_pb.Task{}
        MongoToProto(result,&pout, true)
        out <- pout
      }
      close(out)
    }()
    return out
  }
  //collection.Find(bson.M{"state": id})
  out := make(chan agro_pb.Task)
  defer close(out)
  return out
}