
package agro_db


import (
  "log"
  "fmt"
  //"strings"
  //"reflect"
  "gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
  "agro/proto"
  "encoding/json"
  proto "github.com/golang/protobuf/proto"
  proto_json "github.com/golang/protobuf/jsonpb"
)


type MongoInterface struct {
  url string
  session *mgo.Session
  db *mgo.Database
  openFiles map[string]*mgo.GridFile
}

func ProtoToMongo(pb proto.Message, idFix bool) map[string]interface{} {
    marshaler := proto_json.Marshaler{}
    s, _ := marshaler.MarshalToString(pb)
    out := make(map[string]interface{})
    json.Unmarshal([]byte(s), &out)
    if idFix {
      if _, ok := out["ID"]; ok {
        out["_id"] = out["ID"]
        delete(out, "ID")
      }
    }
    return out
}

func MongoToProto(in map[string]interface{}, pb proto.Message, idFix bool) {
    if idFix {
      if _, ok := in["_id"]; ok {
        in["ID"] = in["_id"]
        delete(in, "_id")
      }
    }
    s, _ := json.Marshal(in)
    proto_json.UnmarshalString(string(s), pb)
}

/*
func ProtoToMongo(pb proto.Message, idFix bool) map[string]interface{} {
  ref := reflect.ValueOf(pb)
  out := make(map[string]interface{})
  s := ref.Elem()
  for i := 0; i < s.NumField(); i++ {
    value := s.Field(i)
    valueField := s.Type().Field(i)
    if !strings.HasPrefix(valueField.Name, "XXX_") && !strings.HasPrefix(valueField.Name, "xxx_") {
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
    if strings.HasPrefix(ft.Name, "XXX_") || strings.HasPrefix(ft.Name, "xxx_") {
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
  if val.Type().Kind() == reflect.Ptr && in == nil {
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
  if val.Type().Kind() == reflect.Slice {
    if (in != nil) { 
      slc := in.([]interface{})
      len := len(slc)
      val.Set(reflect.MakeSlice(val.Type(), len, len))
  		for i := 0; i < len; i++ {
  			mongoToValue(slc[i], val.Index(i))
      }
    }
    return
  }
  if val.Type().Kind() == reflect.Uint64 || val.Type().Kind() == reflect.Int64 {
    val.SetInt( toInt64(in) )
    return
  }
  if val.Type().Kind() == reflect.Uint32 || val.Type().Kind() == reflect.Int32 {
    val.SetInt( toInt64(in) )
    return
  }
  if val.Type().Kind() == reflect.Interface {
    log.Printf("%s", val.Type())
    log.Printf("%s", val.Type().Kind())    
    log.Printf("%s", proto.GetProperties(val.Type()))
  }
  log.Printf("Unknown proto type %s", val.Type().Kind())
}

func toInt64(in interface{}) int64 {
  switch (reflect.ValueOf(in).Type().Kind()) {
  case reflect.Int:
    return int64( in.(int) )
  case reflect.Int32:
    return int64( in.(int32) )
  case reflect.Uint32:
    return int64( in.(uint32) )
  case reflect.Int64:
    return int64( in.(int64) )
  case reflect.Uint64:
    return int64( in.(uint64) )
  case reflect.Float32:
    return int64( in.(float32) )
  case reflect.Float64:
    return int64( in.(float64) )
  }
  fmt.Println("Oh noes:", in, reflect.ValueOf(in).Type().Kind())
  return 0
}
*/

func NewMongo(url string) (*MongoInterface, error) {
  session, err := mgo.Dial(url)
  if err != nil {
    return nil, err
  }
  
  return &MongoInterface{
    url:url,
    session:session,
    db:session.DB("agro"),
    openFiles:make(map[string]*mgo.GridFile),
  }, nil
}

func (self *MongoInterface) AddTask(task *agro_pb.Task) error {
  e := ProtoToMongo(task, true)
  if task.State == nil {
    a := agro_pb.State_QUEUED
    task.State = &a
  }
  return self.db.C("task").Insert(e)
}

func (self *MongoInterface) AddJob(job *agro_pb.Job) error {
  e := ProtoToMongo(job, true)
  return self.db.C("job").Insert(e)
}

func (self *MongoInterface) GetJob(jobID string) *agro_pb.Job {
  result := make(map[string]interface{})
  err := self.db.C("job").Find( bson.M{"_id" : jobID} ).One(&result)
  if err != nil {
    return nil
  }
  out := &agro_pb.Job{}
  MongoToProto(result, out, true)
  fmt.Println("GetJob:", result, out)
  return out
}


func (self *MongoInterface) TaskQuery(state *agro_pb.State) chan agro_pb.Task {
  if *state == agro_pb.State_QUEUED {
    //log.Printf("Scanning For QUEUED tasks")
    out := make(chan agro_pb.Task)
    go func() {
      i := self.db.C("task").Find( 
          bson.M{
            "State" : agro_pb.State_QUEUED.String(),
          }).Iter()
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

func (self *MongoInterface) JobQuery(state *agro_pb.State) chan agro_pb.Job {
  if *state == agro_pb.State_QUEUED {
    //log.Printf("Scanning For QUEUED jobs")
    out := make(chan agro_pb.Job)
    go func() {
      i := self.db.C("job").Find( bson.M{"State" : agro_pb.State_QUEUED.String()} ).Iter()
      result := make(map[string]interface{})
      for i.Next(&result) {
        pout := agro_pb.Job{}
        MongoToProto(result,&pout, true)
        out <- pout
      }
      close(out)
    }()
    return out
  }
  //collection.Find(bson.M{"state": id})
  out := make(chan agro_pb.Job)
  defer close(out)
  return out
}

func (self *MongoInterface) SearchTasks(tags *agro_pb.TagArray) chan agro_pb.TaskInfo {
  out := make(chan agro_pb.TaskInfo)
  go func() {
    var iter *mgo.Iter = nil
    if (len(tags.Tags) > 0) {
      iter = self.db.C("task").Find( bson.M{"tags":bson.M{"$all" : tags.Tags } }).Iter()
    } else {
      iter = self.db.C("task").Find( nil ).Iter()      
    }
    result := make(map[string]interface{})    
    for iter.Next(&result) {
      pout := agro_pb.Task{}
      MongoToProto(result,&pout, true)
      out <- agro_pb.TaskInfo{
        ID:pout.ID,
      }
    }
    close(out)
  } ()
  return out
}

func (self *MongoInterface) GetTask(taskID string) agro_pb.Task {
  result := make(map[string]interface{})  
  self.db.C("task").Find( bson.M{"_id" : taskID}).One(&result)
  out := agro_pb.Task{}
  MongoToProto(result, &out, true)
  return out
}

func (self *MongoInterface) GetTaskJobs(taskID string) chan agro_pb.Job {
  out := make(chan agro_pb.Job)
  go func() {
    var iter = self.db.C("job").Find( bson.M{"TaskID" : taskID} ).Iter()
    result := make(map[string]interface{})    
    for iter.Next(&result) {
      pout := agro_pb.Job{}
      MongoToProto(result,&pout, true)
      out <- pout
    }
    close(out)
  } ()
  return out
}
 
func (self *MongoInterface) GetTaskStatus(taskID string) agro_pb.TaskStatus { 
  //task := self.GetTask(taskID)
  runs := make([]string, 0, 10)
  var completed *string = nil
  var state = agro_pb.State_QUEUED
  for job := range self.GetTaskJobs(taskID) {
    runs = append(runs, *job.ID)
    if *job.State == agro_pb.State_OK {
      completed = job.ID
      state = *job.State
    }
    if state != agro_pb.State_OK {
      if state != agro_pb.State_RUNNING {
        if *job.State == agro_pb.State_RUNNING {
          state = *job.State
        }
      } else {
        state = agro_pb.State_QUEUED
      }
    }
  }
  
  out := &agro_pb.TaskStatus{
    ID: &taskID,
    State: &state,
    CompletedJob: completed,
    Runs: runs,
  }
  return *out
  /*
  required string ID = 1;  
  required State State = 2;
  optional string CompletedJob = 6;
  repeated string Runs = 7;  
  */
}

func (self *MongoInterface) SetJobLogs(jobID string,stdout []byte,stderr []byte) {
  self.db.C("job").Update(bson.M{"_id": jobID}, bson.M{ 
    "$set" : bson.M{
      "Stdout" : string(stdout),
      "Stderr" : string(stderr),
    },
  })
}

func (self *MongoInterface) UpdateJobState(jobID string, state agro_pb.State) {
  log.Printf("Job %s to %s", jobID, state)
  self.db.C("job").Update(bson.M{"_id" : jobID}, bson.M{"$set" : bson.M{"State" : state.String()}})
}

func (self *MongoInterface) GetJobState(jobID string) *agro_pb.State {
  result := make(map[string]interface{})
  err := self.db.C("job").Find( bson.M{"_id" : jobID} ).One(&result)
  if err != nil {
    return nil
  }
  a := agro_pb.State(agro_pb.State_value[result["State"].(string)])
  return &a
}

func (self *MongoInterface) UpdateTaskState(taskID string, state agro_pb.State) {
  log.Printf("Task %s to %s", taskID, state)
  self.db.C("task").Update(bson.M{"_id" : taskID}, bson.M{"$set" : bson.M{"State" : state.String()}})
}

func (self *MongoInterface) CreateFile(info agro_pb.FileInfo) agro_pb.FileState {
  f,_ := self.db.GridFS("fs").Create(*info.Name)
  f.SetId(info.ID)
  f.SetChunkSize(16777216)
  self.openFiles[*info.ID] = f
  return agro_pb.FileState{ State:agro_pb.State_RUNNING.Enum() }
}

func (self *MongoInterface) WriteFile(block agro_pb.DataBlock) agro_pb.FileState {
  self.openFiles[*block.ID].Write(block.Data)
  log.Printf("Writing: %s", block.Data)
  return agro_pb.FileState{ State:agro_pb.State_RUNNING.Enum() }
}

func (self *MongoInterface) CommitFile(f agro_pb.FileID) agro_pb.FileState {
  self.openFiles[*f.ID].Close()
  delete(self.openFiles, *f.ID)
  return agro_pb.FileState{ State:agro_pb.State_OK.Enum() }
}

func (self *MongoInterface) GetFileInfo(i agro_pb.FileID) agro_pb.FileInfo {
  f,_ := self.db.GridFS("fs").OpenId(*i.ID)
  o := agro_pb.FileInfo{
    Name: proto.String(f.Name()),
    ID:   proto.String(f.Id().(string)),
    Size: proto.Int64(f.Size()),
  }
  f.Close()
  return o
}

func (self *MongoInterface) ReadFile(req agro_pb.ReadRequest) agro_pb.DataBlock {
  f,_ := self.db.GridFS("fs").OpenId(*req.ID)
  f.Seek(*req.Start, 0)
  data := make([]byte, *req.Size)
  l, _ := f.Read(data)
  log.Printf("GridRead: %d (%d)", l, *req.Size)
  o := agro_pb.DataBlock{
    ID:req.ID,
    Start:req.Start,
    Len:proto.Int64(int64(l)),
    Data:data[:l],
  }
  f.Close()
  return o
}
