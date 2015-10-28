
package agro_engine

import (
  "log"
  "agro/db"
  "agro/proto"
)

type JobManager interface {
  GetReadyJobCount() int
  GetComputeCount() int
  GetJobState(string) *agro_pb.State
  AddJob(agro_pb.Job)
}

type WorkEngine struct {
  workQueue chan func()
  dbi agro_db.AgroDB
  manager JobManager
}

func (self *WorkEngine) listenQueue() {
  for req := range self.workQueue {
    req()
  }
}

func (self *WorkEngine) QueueWork(work func()) {
  self.workQueue <- work
}

func (self *WorkEngine) AddTask(task agro_pb.Task) error {
  err := self.dbi.AddTask(&task)
  if err != nil { return err }
  if self.manager.GetReadyJobCount() < self.manager.GetComputeCount() {
    self.QueueWork( self.TaskScan )
  }
  return nil
}

func (self *WorkEngine) CreateTaskJob(task *agro_pb.Task) {
  job := agro_pb.Job{
    Info:task,
  }
  self.manager.AddJob(job)
}

func (self *WorkEngine) TaskScan() {
  log.Printf("Scanning For Tasks")
  v := agro_pb.State_QUEUED
  i := 0
  for task := range self.dbi.TaskQuery(&v) {
    if self.manager.GetJobState(*task.ID) == nil {
      go self.QueueWork( func() {self.CreateTaskJob(&task)} )
      i += 1
    }
  }
  log.Printf("Found %d new Tasks", i)
}

func NewEngine(dbi agro_db.AgroDB, manager JobManager, numWorkers int) (WorkEngine, error) {
  workQueue := make(chan func(), 200)
  out := WorkEngine{
    workQueue:workQueue,
    dbi:dbi,
    manager:manager,
  }
  out.QueueWork( out.TaskScan )
  for i := 0; i < numWorkers; i++ {
    go func() {
      out.listenQueue()
    }()
  }
  return out, nil
}