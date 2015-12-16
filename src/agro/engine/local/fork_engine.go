
package agro_local

import (
  "agro/engine"
  "agro/proto"
  "time"
  "log"
)

type ForkManager struct {
  procCount int
  running bool
  engine *agro_engine.WorkEngine
  workdir string
}


func (self *ForkManager) worker(inchan chan agro_pb.Job) {
  for job := range inchan {
    log.Printf("Launch job: %s", job)
    self.engine.GetDBI().UpdateJobState(*job.Id, agro_pb.State_RUNNING)
    err := agro_engine.RunJob(&job, self.workdir, self.engine.GetDBI())
    if err != nil {
      self.engine.FinishJob(*job.Id, agro_pb.State_ERROR)
    } else {
      self.engine.FinishJob(*job.Id, agro_pb.State_OK)
    }
  }
}

func (self *ForkManager) watcher(engine *agro_engine.WorkEngine) {
  self.engine = engine
  jobchan := make(chan agro_pb.Job, 10)
  for i := 0; i < self.procCount; i++ {
    go self.worker(jobchan)
  }
  var sleep_size int64 = 1
  for self.running { 
    job := engine.GetJobToRun()
    if job != nil {
        sleep_size = 1
        log.Printf("Found job: %s", job)
        jobchan <- *job
    } else {
      //log.Printf("No jobs found")      
      if (sleep_size < 20) {
      //  sleep_size += 1
      }    
      time.Sleep(time.Second * time.Duration(sleep_size))
    }
  } 
  close(jobchan)
}

func (self *ForkManager) Start(engine *agro_engine.WorkEngine) {
  go self.watcher(engine)
}

func NewLocalManager(procCount int, workdir string) (*ForkManager, error) {
  return &ForkManager{
    procCount:procCount,
    running:true,
    workdir:workdir,
  }, nil
}