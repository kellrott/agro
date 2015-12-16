
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
  jobCache map[string]*agro_pb.Job
}

/* Engine Create and Start */
func (self *WorkEngine) Start() {
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
    jobCache:make(map[string]*agro_pb.Job),
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
  s := agro_pb.State_WAITING
  job := &agro_pb.Job{
    Id: proto.String(u.String()),
    TaskId:task.Id,
    State:&s,
    Command : task.Command,
    Container: task.Container,
    Args: task.Args,
  }
  log.Printf("Creating job %s", job)
  self.dbi.AddJob(job)
  self.jobCache[*task.Id] = job
}


func (self *WorkEngine) WorkScan() {
  self.JobScan()
  if len(self.jobCache) == 0 {  
    self.GenerateTaskJobs()
  }
}


func (self *WorkEngine) JobScan() {
  //self.dbi.JobQueryCount(agro_pb.State_QUEUED)
  v := agro_pb.State_WAITING 
  for job := range self.dbi.JobQuery(&v, 10) {
    if _, in1 := self.jobCache[*job.TaskId]; !in1 {
      local := job //copy for loop value to local copy, so the pointer won't be on a changing copy
      self.jobCache[*job.TaskId] = &local
    }
  }
}


func (self *WorkEngine) GenerateTaskJobs() {
  //log.Printf("Scanning For Tasks")
  v := agro_pb.State_QUEUED
  i := 0
  for task := range self.dbi.TaskQuery(&v) {
    complete_count := 0
    var error_count int32 = 0
    for job := range self.dbi.GetTaskJobs(*task.Id) {
        if *job.State == agro_pb.State_OK {
          complete_count += 1
        } else if *job.State == agro_pb.State_ERROR {
          error_count += 1
        }
    }
    if complete_count == 0 {
      if error_count > 0 && (task.MaxRetry == nil || error_count > *task.MaxRetry) {
        self.dbi.UpdateTaskState(*task.Id, agro_pb.State_ERROR)
      } else {
        log.Printf("Found task %s", *task.Id)
        self.CreateTaskJob(&task)
        i += 1
      }
    } else {
      self.dbi.UpdateTaskState(*task.Id, agro_pb.State_OK)
    }
  }
  if i > 0 {
    log.Printf("Found %d new Tasks", i)
  }
}

func (self *WorkEngine) FinishJob(jobID string, state agro_pb.State) {
  self.dbi.UpdateJobState(jobID, state)
  job := self.dbi.GetJob(jobID)
  if job == nil {
    log.Printf("Job %s not found", jobID)
    return
  }
  if state == agro_pb.State_OK {
    self.dbi.UpdateTaskState(jobID, agro_pb.State_OK)
    log.Printf("Finishing %s", job)
  }
}

func (self *WorkEngine) GetJobToRun() *agro_pb.Job {
  for id, job := range self.jobCache {
    self.dbi.UpdateTaskState(*job.Id, agro_pb.State_QUEUED)
    delete(self.jobCache, id)
    return job
  }
  go self.WorkScan()
  return nil
}

func (self *WorkEngine) GetDBI() agro_db.AgroDB {
  return self.dbi
}
