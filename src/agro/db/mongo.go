
package agro_db


import (
  "agro"
  "log"
  "fmt"
  "gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
  "agro/proto"
  uuid "github.com/nu7hatch/gouuid"
  proto "github.com/golang/protobuf/proto"
  context "golang.org/x/net/context"
  "strconv"
  "time"
)


type MongoInterface struct {
  url string
  session *mgo.Session
  db *mgo.Database
  openFiles map[string]*mgo.GridFile
  jobCache map[string]*agro_pb.Job
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
    openFiles:make(map[string]*mgo.GridFile),
    jobCache:make(map[string]*agro_pb.Job),
  }, nil
}

func (self *MongoInterface) AddTask(ctx context.Context, task *agro_pb.Task) (*agro_pb.TaskStatus, error) {
  task.State = agro_pb.State_WAITING.Enum()
  e := agro.ProtoToMap(task, true)
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
  e := agro.ProtoToMap(job, true)
  return self.db.C("job").Insert(e)
}

func (self *MongoInterface) GetJob(ctx context.Context, jobID *agro_pb.IDQuery) (*agro_pb.Job, error) {
  return self.getJob(jobID.Ids[0]), nil
}

func (self *MongoInterface) getJob(jobID string) *agro_pb.Job {
  result := make(map[string]interface{})
  err := self.db.C("job").Find( bson.M{"_id" : jobID} ).One(&result)
  if err != nil {
    return nil
  }
  out := &agro_pb.Job{}
  agro.MapToProto(result, out, true)
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
        agro.MapToProto(result,&pout, true)
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
      agro.MapToProto(result,&pout, true)
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
    agro.MapToProto(result, &pout, true)
    stream.Send(&pout)
  }
  return nil
}


func (self *MongoInterface) getTask(id string) agro_pb.Task {
  result := make(map[string]interface{})
  self.db.C("task").Find( bson.M{"_id" : id}).One(&result)
  out := agro_pb.Task{}
  agro.MapToProto(result, &out, true)
  return out
}

func (self *MongoInterface) GetTask(ctx context.Context, taskID *agro_pb.IDQuery) (*agro_pb.Task, error) {
  c := self.getTask(taskID.Ids[0])
  return &c, nil
}

func (self *MongoInterface) GetTaskJobs(taskID string) chan agro_pb.Job {
  out := make(chan agro_pb.Job)
  go func() {
    var iter = self.db.C("job").Find( bson.M{"task_id" : taskID} ).Iter()
    result := make(map[string]interface{})    
    for iter.Next(&result) {
      pout := agro_pb.Job{}
      agro.MapToProto(result,&pout, true)
      out <- pout
    }
    close(out)
  } ()
  return out
}
 
func (self *MongoInterface) GetTaskStatus(ids *agro_pb.IDQuery, stream agro_pb.Scheduler_GetTaskStatusServer) error {
  i_task := self.db.C("task").Find( bson.M{"_id" : bson.M{ "$in" : ids.Ids} } ).Iter()
  result := make(map[string]interface{})
  for i_task.Next(&result) {

    i_job := self.db.C("job").Find( bson.M{"task_id" : result["_id"]} ).Iter()
    runs := make([]string, 0)
    var completed *string = nil

    job_result := make(map[string]interface{})
    for i_job.Next(&job_result) {
      runs = append(runs, string(job_result["_id"].(string)))
      if job_result["state"] == "OK" {
        s := string(job_result["_id"].(string))
        completed = &s
      }
    }
    s := agro_pb.State(agro_pb.State_value[result["state"].(string)])
    a := &agro_pb.TaskStatus{
      Id: proto.String(string(result["_id"].(string))),
      State:&s,
      Runs: runs,
      CompletedJob:completed,
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
  job := self.getJob(*req.Id)
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


func (self *MongoInterface) WorkerPing(context context.Context, worker *agro_pb.WorkerInfo) (*agro_pb.WorkerInfo, error) {
  t := time.Now().Unix()
  worker.LastPing = &t
  w := agro.ProtoToMap(worker, true)
  self.db.C("worker").UpsertId( worker.Id, w )
  return worker, nil
}

func (self *MongoInterface) SearchWorkers(tags *agro_pb.TagArray, stream agro_pb.Scheduler_SearchWorkersServer) error {
  var iter *mgo.Iter = nil
  if (len(tags.Tags) > 0) {
    iter = self.db.C("worker").Find( bson.M{"tags":bson.M{"$all" : tags.Tags } }).Iter()
  } else {
    iter = self.db.C("worker").Find( nil ).Iter()
  }
  result := make(map[string]interface{})
  for iter.Next(&result) {
    out := agro_pb.WorkerInfo{}
    agro.MapToProto(result, &out, true)
    stream.Send(&out)
  }
  return nil
}


func (self *MongoInterface) pingWorker(worker_id string) {
  self.db.C("worker").UpsertId(worker_id, bson.M{ "last_ping" : time.Now().Unix() })
}

func (self *MongoInterface) CreateFile(ctx context.Context, info *agro_pb.FileInfo) (*agro_pb.FileState, error) {
  f,_ := self.db.GridFS("fs").Create(*info.Name)
  f.SetId(info.Id)
  f.SetChunkSize(15728640)
  self.openFiles[*info.Id] = f
  return &agro_pb.FileState{ State:agro_pb.State_RUNNING.Enum() }, nil
}

func (self *MongoInterface) WriteFile(ctx context.Context, block *agro_pb.DataBlock) (*agro_pb.FileState, error) {
  written, err := self.openFiles[*block.Id].Write(block.Data)
  if err != nil {
    log.Println(err)
    return nil, err
  }
  log.Printf("Writing: %d of %d", written, len(block.Data))
  return &agro_pb.FileState{ State:agro_pb.State_RUNNING.Enum() }, nil
}

func (self *MongoInterface) CommitFile(ctx context.Context, f *agro_pb.FileID) (*agro_pb.FileState, error) {
  self.openFiles[*f.Id].Close()
  delete(self.openFiles, *f.Id)
  a := agro_pb.FileState{ State:agro_pb.State_OK.Enum() }
  return &a, nil
}

func (self *MongoInterface) GetFileInfo(ctx context.Context, i *agro_pb.FileID) (*agro_pb.FileInfo, error) {
  f, err := self.db.GridFS("fs").OpenId(*i.Id)
  if err != nil {
    return &agro_pb.FileInfo{
      Name: proto.String(fmt.Sprintf("%s error: %s", *i.Id, err)),
      Id:   i.Id,
      State: agro_pb.State_ERROR.Enum(),
    }, nil
  }
  o := agro_pb.FileInfo{
    Name: proto.String(f.Name()),
    Id:   proto.String(f.Id().(string)),
    Size: proto.Int64(f.Size()),
    State: agro_pb.State_OK.Enum(),
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

func (self *MongoInterface) DeleteFile(ctx context.Context, req *agro_pb.FileID) (*agro_pb.FileState, error) {
  err := self.db.GridFS("fs").RemoveId(*req.Id)
  if err != nil {
    return &agro_pb.FileState{
      State: agro_pb.State_ERROR.Enum(),
    }, nil
  }
  return &agro_pb.FileState{
    State: agro_pb.State_OK.Enum(),
  }, nil
}

func (self *MongoInterface) CreateTaskJob(task *agro_pb.Task) {
  u, _ := uuid.NewV4() 
  job := &agro_pb.Job{
    Id: proto.String(u.String()),
    TaskId:task.Id,
    State:agro_pb.State_WAITING.Enum(),
    Command : task.Command,
    Container: task.Container,
    Requirements: task.Requirements,
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
      t := self.getTask(i)
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
  w := agro.ProtoToMap(request.Worker, true)
  self.db.C("worker").UpsertId( request.Worker.Id, w )
  for id, job := range self.jobCache {
    self.UpdateJobState(context.Background(),
      &agro_pb.UpdateStateRequest{
        Id:job.Id,
        State:agro_pb.State_QUEUED.Enum(),
        WorkerId:request.Worker.Id,
      },
    )

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


/**
 * Document Interface
 */

func populate_obj(data interface{}, path []string, field *agro_pb.Field) {
  //log.Printf("Populate: %#v %#v %#v", path, data, field)
  if len(path) > 1 {
    if v, ok := data.(map[string]interface{}); ok {
      //log.Printf("Entering map %#v", data)
      if a, ok := v[path[0]].(map[string]interface{}); ok {
        populate_obj(a, path[1:], field)
      } else if a, ok := v[path[0]].([]interface{}); ok {
        populate_obj(&a, path[1:], field)
      } else {
        log.Printf("Unpacking Weird Data")
      }
    } else {
      if v, ok := data.(*[]interface{}); ok {
        //log.Printf("Entering Array %#v", data)
        i, _ := strconv.Atoi(path[0])
        if a, ok := (*v)[i].(map[string]interface{}); ok {
          populate_obj(a, path[1:], field)
        } else if a, ok := (*v)[i].([]interface{}); ok {
          populate_obj(&a, path[1:], field)
        } else {
          log.Printf("Unpacking Weird Data")
        }
      }
    }
  } else if len(path) == 1 {
    //log.Printf("Setting: %#v %#v", path, field)
    if v, ok := field.Value.(*agro_pb.Field_ListDeclare); ok {
      if t, ok := (data).(*[]interface{}); ok {
        i, _ := strconv.Atoi(path[0])
        (*t)[i] = make([]interface{}, v.ListDeclare, v.ListDeclare)
      } else if t, ok := (data).(map[string]interface{}); ok {
        (t)[path[0]] = make([]interface{}, v.ListDeclare, v.ListDeclare)
      } else {
        log.Printf("Unpacking Weird Data")
      }
    } else if v, ok := field.Value.(*agro_pb.Field_MapDeclare); ok {
      //log.Printf("Map Declare")
      if t, ok := (data).(*[]interface{}); ok {
        i, _ := strconv.Atoi(path[0])
        (*t)[i] = make(map[string]interface{}, v.MapDeclare)
      } else if t, ok := (data).(map[string]interface{}); ok {
        (t)[path[0]] = make(map[string]interface{}, v.MapDeclare)
      } else {
        log.Printf("Unpacking Weird Data")
      }
    } else {
      var i interface{} = nil
      if v, ok := field.Value.(*agro_pb.Field_BoolValue); ok {
        i = v.BoolValue
      } else if v, ok := field.Value.(*agro_pb.Field_StrValue); ok {
        i = v.StrValue
      } else if v, ok := field.Value.(*agro_pb.Field_FloatValue); ok {
        i = v.FloatValue
      } else if v, ok := field.Value.(*agro_pb.Field_IntValue); ok {
        i = v.IntValue
      } else {
        i = nil
      }
      if v, ok := (data).(*[]interface{}); ok {
        c, _ := strconv.Atoi(path[0])
        (*v)[c] = i
      } else if v, ok := (data).(map[string]interface{}); ok {
        (v)[path[0]] = i
      } else {
        log.Printf("Unpacking Weird Data")
      }
    }
  }
}

func (self *MongoInterface) CreateDoc(ctx context.Context, doc *agro_pb.Document) (*agro_pb.FileState, error) {
  o := UnpackDoc(doc)
  self.db.C("doc").Insert(o)
  return &agro_pb.FileState{ State:agro_pb.State_OK.Enum() }, nil
}

func (self *MongoInterface) DeleteDoc(ctx context.Context, doc *agro_pb.FileID) (*agro_pb.FileState, error) {
  self.db.C("doc").RemoveId(doc.Id)
  return &agro_pb.FileState{ State:agro_pb.State_OK.Enum() }, nil
}

func quick_copy(src []string) []string {
  dst := make([]string, len(src))
  copy(dst, src)
  return dst
}

func pack_data(data interface{}, prefix []string, output chan agro_pb.Field) {
  switch f := data.(type) {
  case map[string]interface{}:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_MapDeclare{MapDeclare: int64(len(f))}}
    for key, value := range f {
      pack_data(value, append(prefix, key), output)
    }
  case []interface{}:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_ListDeclare{ListDeclare: int64(len(f))}}
    for i, v := range f {
      a := strconv.Itoa(i)
      pack_data(v, append(prefix, a), output)
    }
  case string:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_StrValue{StrValue:f} }
  case int:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_IntValue{IntValue:int64(f)} }
  case int32:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_IntValue{IntValue:int64(f)} }
  case int64:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_IntValue{IntValue:int64(f)} }
  case float32:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_FloatValue{FloatValue:float64(f)} }
  case float64:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_FloatValue{FloatValue:float64(f)} }
  case bool:
    output <- agro_pb.Field{Path:quick_copy(prefix), Value:&agro_pb.Field_BoolValue{BoolValue:f} }
  case nil:
    output <- agro_pb.Field{Path:quick_copy(prefix)}
  default:
    log.Printf("Packing unknown type: %T %#v", data, data)
  }
}


func StreamFields(doc map[string]interface{}) chan agro_pb.Field {
  f_chan := make(chan agro_pb.Field)
  go func() {
    pack_data(doc, []string{}, f_chan)
    close(f_chan)
  } ()
  return f_chan
}

func PackDoc(id string, doc map[string]interface{}) *agro_pb.Document {
  fields := make([]*agro_pb.Field,0)
  for f := range StreamFields(doc) {
    a := f
    if len(a.Path) != 1 || a.Path[0] != "_id" {
      fields = append(fields, &a)
    }
  }

  //for _, f := range fields {
  //  log.Printf("Got: %#v %s", f.Path, f.Value)
  //}

  //log.Printf("OutFields: %d", len(fields))
  return &agro_pb.Document{Id:proto.String(id), Fields:fields}
}


func UnpackDoc(doc *agro_pb.Document) map[string]interface{} {
  //populate the map
  o := make(map[string]interface{})
  //log.Printf("In fields: %d", len(doc.Fields))
  for _, field := range doc.Fields {
    populate_obj(o, field.Path, field)
  }
  o["_id"] = doc.Id
  return o
}


func (self *MongoInterface) GetDoc(ctx context.Context, doc_id *agro_pb.FileID) (*agro_pb.Document, error) {
  result := make(map[string]interface{})
  self.db.C("doc").Find( bson.M{"_id" : doc_id.Id}).One(&result)

  doc := PackDoc(*doc_id.Id, result)
  return doc, nil
}


func (self *MongoInterface) UpdateDoc(ctx context.Context, doc *agro_pb.Document) (*agro_pb.FileState, error) {
  o := UnpackDoc(doc)
  self.db.C("doc").UpdateId(doc.Id, o)
  return &agro_pb.FileState{ State:agro_pb.State_OK.Enum() }, nil
}