
package agro_db


import (
  "log"
  "fmt"
  //"strings"
  //"reflect"
  uuid "github.com/nu7hatch/gouuid"
  "gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
  "agro/proto"
  "encoding/json"
  proto "github.com/golang/protobuf/proto"
  proto_json "github.com/golang/protobuf/jsonpb"
  context "golang.org/x/net/context"
)


type MongoInterface struct {
  url string
  session *mgo.Session
  db *mgo.Database
  openFiles map[string]*mgo.GridFile
  jobCache map[string]*agro_pb.Job
}

func ProtoToMongo(pb proto.Message, idFix bool) map[string]interface{} {
    marshaler := proto_json.Marshaler{}
    s, _ := marshaler.MarshalToString(pb)
    out := make(map[string]interface{})
    json.Unmarshal([]byte(s), &out)
    if idFix {
      if _, ok := out["id"]; ok {
        out["_id"] = out["id"]
        delete(out, "id")
      }
    }
    return out
}

func MongoToProto(in map[string]interface{}, pb proto.Message, idFix bool) {
    if idFix {
      if _, ok := in["_id"]; ok {
        in["id"] = in["_id"]
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
    jobCache:make(map[string]*agro_pb.Job),
  }, nil
}

func (self *MongoInterface) AddTask(ctx context.Context, task *agro_pb.Task) (*agro_pb.TaskStatus, error) {
  task.State = agro_pb.State_WAITING.Enum()
  e := ProtoToMongo(task, true)
  err := self.db.C("task").Insert(e)
  s := agro_pb.State_OK
  if err != nil {
    s = agro_pb.State_ERROR
  }
  o := &agro_pb.TaskStatus{
    Id:task.Id,
    State: &s,
  }
  return o, err
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
  return out
}


func (self *MongoInterface) TaskQuery(states []agro_pb.State) chan agro_pb.Task {
    out := make(chan agro_pb.Task)
    go func() {
      qSet := make([]string, len(states))
      for _, s := range states { qSet = append(qSet, s.String()) }
      i := self.db.C("task").Find( 
          bson.M{
            "state" : bson.M{ "$in" : qSet },
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

func (self *MongoInterface) JobQuery(state *agro_pb.State, max int) chan agro_pb.Job {
  out := make(chan agro_pb.Job)
  count := 0
  go func() {
    i := self.db.C("job").Find( bson.M{"state" : state.String()} ).Iter()
    result := make(map[string]interface{})
    for i.Next(&result) {
      pout := agro_pb.Job{}
      MongoToProto(result,&pout, true)
      out <- pout
      count += 1 
      if max > 0 && count >= max {
        break
      }
    }
    close(out)
  }()
  return out
}

func (self *MongoInterface) SearchTasks(tags *agro_pb.TagArray, stream agro_pb.Scheduler_SearchTasksServer) error {
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
    stream.Send(&pout)
  }
  return nil
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
    var iter = self.db.C("job").Find( bson.M{"task_id" : taskID} ).Iter()
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
 
func (self *MongoInterface) GetTaskStatus(ids *agro_pb.IDQuery, stream agro_pb.Scheduler_GetTaskStatusServer) error {
  /*
  runs := make([]string, 0, 10)
  var completed *string = nil
  var state = agro_pb.State_QUEUED
  for job := range self.GetTaskJobs(taskID) {
    runs = append(runs, *job.Id)
    if *job.State == agro_pb.State_OK {
      completed = job.Id
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
    Id: &taskID,
    State: &state,
    CompletedJob: completed,
    Runs: runs,
  }
  return *out
  */
  /*
  required string ID = 1;  
  required State State = 2;
  optional string CompletedJob = 6;
  repeated string Runs = 7;  
  */

  i := self.db.C("task").Find( bson.M{"_id" : bson.M{ "$in" : ids.Ids} } ).Iter()
  result := make(map[string]interface{})
  for i.Next(&result) {
    s := agro_pb.State(agro_pb.State_value[result["state"].(string)])
    a := &agro_pb.TaskStatus{
      Id: proto.String(string(result["_id"].(string))),
      State:&s,
    }
    stream.Send(a)
  }
  return nil
}

func (self *MongoInterface) SetJobLogs(ctx context.Context, log *agro_pb.JobLog) (*agro_pb.JobStatus, error) {
  self.db.C("job").Update(bson.M{"_id": log.Id}, bson.M{
    "$set" : bson.M{
      "stdout" : string(log.Stdout),
      "stderr" : string(log.Stderr),
    },
  })
  return &agro_pb.JobStatus{Id:log.Id, State:agro_pb.State_OK.Enum()}, nil
}

func (self *MongoInterface) UpdateJobState(ctx context.Context, req *agro_pb.UpdateStateRequest) (*agro_pb.JobStatus, error) {
  job := self.GetJob(*req.Id)
  if job == nil {
    log.Printf("Job %s not found", req.Id)
    return nil, fmt.Errorf("Job %s not found", req.Id)
  }
  log.Printf("Job %s to %s", *req.Id, req.State)
  u := bson.M{"$set" : bson.M{"state" : req.State.String()}}
  if req.WorkerId != nil {
    u = bson.M{"$set" : bson.M{"state" : req.State.String(), "worker_id" : *req.WorkerId }}
  }
  self.db.C("job").Update(bson.M{"_id" : req.Id}, u)
  if *req.State == agro_pb.State_OK {
    self.UpdateTaskState(*req.Id, agro_pb.State_OK)
    log.Printf("Finishing %s", job)
  }
  return &agro_pb.JobStatus{
    Id:req.Id,
    State:req.State,
  }, nil
}

func (self *MongoInterface) GetJobStatus(ids *agro_pb.IDQuery, stream agro_pb.Scheduler_GetJobStatusServer) error {
  i := self.db.C("job").Find( bson.M{"_id" : bson.M{ "$in" : ids.Ids} } ).Iter()
  result := make(map[string]interface{})
  for i.Next(&result) {
    s := agro_pb.State(agro_pb.State_value[result["state"].(string)])
    a := &agro_pb.JobStatus{
      Id: proto.String(string(result["_id"].(string))),
      State:&s,
    }
    stream.Send(a)
  }
  return nil
}

func (self *MongoInterface) UpdateTaskState(taskID string, state agro_pb.State) {
  log.Printf("Task %s to %s", taskID, state)
  self.db.C("task").Update(bson.M{"_id" : taskID}, bson.M{"$set" : bson.M{"state" : state.String()}})
}

func (self *MongoInterface) CreateFile(ctx context.Context, info *agro_pb.FileInfo) (*agro_pb.FileState, error) {
  f,_ := self.db.GridFS("fs").Create(*info.Name)
  f.SetId(info.Id)
  f.SetChunkSize(16777216)
  self.openFiles[*info.Id] = f
  return &agro_pb.FileState{ State:agro_pb.State_RUNNING.Enum() }, nil
}

func (self *MongoInterface) WriteFile(ctx context.Context, block *agro_pb.DataBlock) (*agro_pb.FileState, error) {
  self.openFiles[*block.Id].Write(block.Data)
  log.Printf("Writing: %s", block.Data)
  return &agro_pb.FileState{ State:agro_pb.State_RUNNING.Enum() }, nil
}

func (self *MongoInterface) CommitFile(ctx context.Context, f *agro_pb.FileID) (*agro_pb.FileState, error) {
  self.openFiles[*f.Id].Close()
  delete(self.openFiles, *f.Id)
  a := agro_pb.FileState{ State:agro_pb.State_OK.Enum() }
  return &a, nil
}

func (self *MongoInterface) GetFileInfo(ctx context.Context, i *agro_pb.FileID) (*agro_pb.FileInfo, error) {
  f,_ := self.db.GridFS("fs").OpenId(*i.Id)
  o := agro_pb.FileInfo{
    Name: proto.String(f.Name()),
    Id:   proto.String(f.Id().(string)),
    Size: proto.Int64(f.Size()),
  }
  f.Close()
  return &o, nil
}

func (self *MongoInterface) ReadFile(ctx context.Context, req *agro_pb.ReadRequest) (*agro_pb.DataBlock,error) {
  f,_ := self.db.GridFS("fs").OpenId(*req.Id)
  f.Seek(*req.Start, 0)
  data := make([]byte, *req.Size)
  l, _ := f.Read(data)
  log.Printf("GridRead: %d (%d)", l, *req.Size)
  o := agro_pb.DataBlock{
    Id:req.Id,
    Start:req.Start,
    Len:proto.Int64(int64(l)),
    Data:data[:l],
  }
  f.Close()
  return &o, nil
}


func (self *MongoInterface) CreateTaskJob(task *agro_pb.Task) {
  u, _ := uuid.NewV4() 
  job := &agro_pb.Job{
    Id: proto.String(u.String()),
    TaskId:task.Id,
    State:agro_pb.State_WAITING.Enum(),
    Command : task.Command,
    Container: task.Container,
    Args: task.Args,
  }
  log.Printf("Creating job %s", job)
  self.AddJob(job)
  self.jobCache[*task.Id] = job
}


func (self *MongoInterface) workScan() {
  self.jobScan()
  if len(self.jobCache) == 0 {  
    self.generateTaskJobs()
    go self.scanTaskUpdate()
    go self.generateTaskJobs()
  }
}


func (self *MongoInterface) jobScan() {
  for job := range self.JobQuery(agro_pb.State_WAITING.Enum(), 10) {
    if _, in1 := self.jobCache[*job.TaskId]; !in1 {
      local := job //copy for loop value to local copy, so the pointer won't be on a changing copy
      self.jobCache[*job.TaskId] = &local
    }
  }
}

func (self *MongoInterface) scanTaskUpdate() {
  for task := range self.TaskQuery( []agro_pb.State{agro_pb.State_WAITING} ) {
    ready := true
    for _, i := range task.TaskDepends {
      t := self.GetTask(i)
      if *t.State != agro_pb.State_OK {
        ready = false
      }
    }
    if ready {
      log.Printf("Task %s is READY", task.Id)
      self.UpdateTaskState(*task.Id, agro_pb.State_READY)
    }
  }
}


func (self *MongoInterface) generateTaskJobs() {
  log.Printf("Scanning For Tasks")
  i := 0
  for task := range self.TaskQuery( []agro_pb.State{agro_pb.State_READY} ) {
    complete_count := 0
    active_count := 0
    var error_count int32 = 0
    for job := range self.GetTaskJobs(*task.Id) {
        if *job.State == agro_pb.State_OK {
          complete_count += 1
        } else if *job.State == agro_pb.State_WAITING || *job.State == agro_pb.State_QUEUED || *job.State == agro_pb.State_READY || *job.State == agro_pb.State_RUNNING {
          active_count += 1
        } else if *job.State == agro_pb.State_ERROR {
          error_count += 1
        }
    }
    if complete_count == 0 {
      if error_count > 0 && (task.MaxRetry == nil || error_count > *task.MaxRetry) {
        self.UpdateTaskState(*task.Id, agro_pb.State_ERROR)
      } else {
        if active_count == 0 {
          log.Printf("Found task %s", *task.Id)
          self.CreateTaskJob(&task)
          i += 1
        }
      }
    } else {
      self.UpdateTaskState(*task.Id, agro_pb.State_OK)
    }
  }
  if i > 0 {
    log.Printf("Found %d new Tasks", i)
  }
}

func (self *MongoInterface) GetJobToRun(request *agro_pb.JobRequest, stream agro_pb.Scheduler_GetJobToRunServer) (error) {
  for id, job := range self.jobCache {
    self.UpdateJobState(context.Background(), &agro_pb.UpdateStateRequest{Id:job.Id, State:agro_pb.State_QUEUED.Enum(), WorkerId:request.WorkerId})
    delete(self.jobCache, id)
    log.Printf("Sending job: %s", *job.Id)
    if err := stream.Send(job); err != nil {
      fmt.Printf("Error: %s", err)
      return err
    }
    return nil
  }
  log.Printf("No Jobs Found")
  go self.workScan()
  return nil
}
