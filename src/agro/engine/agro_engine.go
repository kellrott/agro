
package agro_engine

import (
  "log"
  "agro/db"
  "agro/proto"
  "github.com/golang/protobuf/proto"
  uuid "github.com/nu7hatch/gouuid"
)

type JobManager interface {
  Start(*WorkEngine)
}

type WorkEngine struct {
  eventQueue chan func()
  dbi agro_db.AgroDB
  manager JobManager
  numWorkers int
  activeTasks map[string]*agro_pb.Job
  readyJobs map[string]*agro_pb.Job
  runningJobs map[string]*agro_pb.Job
}

/* Engine Create and Start */
func (self *WorkEngine) Start() {
  self.QueueEvent( self.TaskScan )
  for i := 0; i < self.numWorkers; i++ {
    go func() {
      self.listenQueue()
    }()
  }
  self.manager.Start(self)
}

func NewEngine(dbi agro_db.AgroDB, manager JobManager, numWorkers int) (WorkEngine, error) {
  eventQueue := make(chan func(), 200)
  out := WorkEngine{
    eventQueue:eventQueue,
    dbi:dbi,
    manager:manager,
    numWorkers:numWorkers,
    activeTasks:make(map[string]*agro_pb.Job),
    readyJobs:make(map[string]*agro_pb.Job),
    runningJobs:make(map[string]*agro_pb.Job),
  }
  return out, nil
}

/* Event Runners */

func (self *WorkEngine) listenQueue() {
  for req := range self.eventQueue {
    req()
  }
}

func (self *WorkEngine) QueueEvent(work func()) {
  //log.Printf("Queue engine event")
  self.eventQueue <- work
}

/* Engine Events */

func (self *WorkEngine) CreateTaskJob(task *agro_pb.Task) {
  u, _ := uuid.NewV4() 
  s := agro_pb.State_QUEUED
  job := &agro_pb.Job{
    ID: proto.String(u.String()),
    TaskID:task.ID,
    State:&s,
    Command : task.Command,
    Container: task.Container,
    Args: task.Args,
  }
  log.Printf("Creating job %s", job)
  self.dbi.AddJob(job)
}

func (self *WorkEngine) WorkScan() {
  self.JobScan()
  if len(self.readyJobs) == 0 {  
    self.TaskScan()
  }
  if len(self.readyJobs) + len(self.runningJobs) + len(self.activeTasks) > 0 {
    log.Printf("Jobs Ready: %d Running: %d Active: %d", 
      len(self.readyJobs), len(self.runningJobs), len(self.activeTasks))
  }
}

func (self *WorkEngine) JobScan() {
  //self.dbi.JobQueryCount(agro_pb.State_QUEUED)
  v := agro_pb.State_QUEUED  
  for job := range self.dbi.JobQuery(&v) {
    if _, in1 := self.readyJobs[*job.ID]; !in1 {
      if _, in2 := self.runningJobs[*job.ID]; !in2 {
        local := job //copy for loop value to local copy, so the pointer won't be on a changing copy
        self.readyJobs[*job.ID] = &local
        self.activeTasks[*job.TaskID] = &local
      }
    }
  }
}

func (self *WorkEngine) TaskScan() {
  //log.Printf("Scanning For Tasks")
  v := agro_pb.State_QUEUED
  i := 0
  for task := range self.dbi.TaskQuery(&v) {
    if _, ok := self.activeTasks[*task.ID]; !ok {
      log.Printf("Found task %s", *task.ID)
      complete_count := 0
      for job := range self.dbi.GetTaskJobs(*task.ID) {
          if *job.State == agro_pb.State_OK {
            complete_count += 1
          }
      }
      if complete_count == 0 {
        self.CreateTaskJob(&task)
        i += 1
      } else {
        self.UpdateTaskState(*task.ID, agro_pb.State_OK)
      }
    }
  }
  if i > 0 {
    log.Printf("Found %d new Tasks", i)
  }
}


func (self *WorkEngine) FinishJob(jobID string) {
  self.UpdateJobState(jobID, agro_pb.State_OK)
  job := self.dbi.GetJob(jobID)
  if job == nil {
    log.Printf("Job %s not found", jobID)
    return
  }
  self.dbi.UpdateTaskState(jobID, agro_pb.State_OK)
  log.Printf("Finishing %s", job)
  
  //finish the task as well
  taskID := *job.TaskID
  delete(self.runningJobs, jobID)
  delete(self.activeTasks, taskID)
}


/* DBI Proxy */
/*
func (self *WorkEngine) GetReadyJobCount() int {
  return len(self.readyJobs)
}

func (self *WorkEngine) GetRunningJobCount() int {
  return len(self.runningJobs)
}
*/

func (self *WorkEngine) GetJobToRun() *agro_pb.Job {  
  for id, job := range self.readyJobs {
    self.runningJobs[id] = job
    delete(self.readyJobs, id)
    return job
  }
  self.QueueEvent( self.WorkScan )
  return nil
}

func (self *WorkEngine) GetDBI() agro_db.AgroDB {
  return self.dbi
}

/*
DBI interface
*/

func (self *WorkEngine) AddTask(task *agro_pb.Task) error {
  log.Printf("Adding new Task %s", task.ID)
  err := self.dbi.AddTask(task)
  if err != nil { return err }
  /*
  if self.GetReadyJobCount() < 2 { //TODO: Better logic here
    self.QueueEvent( self.WorkScan )
  }
  */
  return nil
}

func (self *WorkEngine) GetTaskJobs(taskID string) chan agro_pb.Job {
  return self.dbi.GetTaskJobs(taskID)
}

func (self *WorkEngine) GetTaskStatus(taskID string) agro_pb.TaskStatus {
  return self.dbi.GetTaskStatus(taskID)
}

func (self *WorkEngine) TaskQuery(state *agro_pb.State) chan agro_pb.Task {
  return self.dbi.TaskQuery(state)
}

func (self *WorkEngine) JobQuery(state *agro_pb.State) chan agro_pb.Job {
  return self.dbi.JobQuery(state)
}

func (self *WorkEngine) SearchTasks(tags *agro_pb.TagArray) chan agro_pb.TaskInfo {
  return self.dbi.SearchTasks(tags)
}


func (self *WorkEngine) GetJob(jobID string) *agro_pb.Job {
  return self.dbi.GetJob(jobID)
}

func (self *WorkEngine) AddJob(job *agro_pb.Job) error {
  self.dbi.AddJob(job)
  return nil
}

func (self *WorkEngine) UpdateTaskState(taskID string, state agro_pb.State) {
  self.dbi.UpdateTaskState(taskID, state)
}
func (self *WorkEngine) UpdateJobState(jobID string, state agro_pb.State) {
  self.dbi.UpdateJobState(jobID, state)
}

func (self *WorkEngine) GetJobState(jobID string) *agro_pb.State {
  return self.dbi.GetJobState(jobID)
  /*
  if _, ok := self.readyJobs[jobID]; ok {
    out :=  agro_pb.State_QUEUED
    return &out
  }
  self.QueueEvent( self.TaskScan )
  return nil
  */
}

func (self *WorkEngine)SetJobLogs(jobID string,stdout []byte,stderr []byte) {
  self.dbi.SetJobLogs(jobID, stdout, stderr)
}
