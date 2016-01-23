
package agro_local

import (
  "agro/engine"
  "agro/proto"
  "time"
  "log"
  "sync/atomic"
  context "golang.org/x/net/context"
  //"github.com/gogo/protobuf/proto"
  proto "github.com/gogo/protobuf/proto"
)


type ForkManager struct {
  procCount int
  running bool
  sched agro_pb.SchedulerClient
  files agro_pb.FileStoreClient
  workdir string
  workerId string
  ctx context.Context
  check_func func(status agro_engine.EngineStatus)
  status agro_engine.EngineStatus
}


func (self *ForkManager) worker(inchan chan agro_pb.Job) {
  for job := range inchan {
    atomic.AddInt32(&self.status.ActiveJobs, 1)
    atomic.AddInt32(&self.status.JobCount, 1)
    log.Printf("Launch job: %s", job)
    s := agro_pb.State_RUNNING
    self.sched.UpdateJobState(self.ctx, &agro_pb.UpdateStateRequest{Id:job.Id, State:&s})
    stdout, stderr, err := agro_engine.RunJob(&job, self.workdir, self.files)
    self.sched.SetJobLogs(self.ctx, &agro_pb.JobLog{Id:job.Id, Stderr:stderr, Stdout:stdout})
    if err != nil {
      log.Printf("Job %s failed", *job.Id)
      s = agro_pb.State_ERROR
    } else {
      log.Printf("Job %s complete", *job.Id)
      s = agro_pb.State_OK
    }
    self.sched.UpdateJobState(self.ctx, &agro_pb.UpdateStateRequest{Id:job.Id, State:&s})
    atomic.AddInt32(&self.status.ActiveJobs, -1)
  }
}

func (self *ForkManager) watcher(sched agro_pb.SchedulerClient, filestore agro_pb.FileStoreClient) {
  self.sched = sched
  self.files = filestore
  jobchan := make(chan agro_pb.Job, 10)
  for i := 0; i < self.procCount; i++ {
    go self.worker(jobchan)
  }
  var sleep_size int64 = 1
  for self.running {
    if self.check_func != nil {
      self.check_func(self.status)
    }
    job_stream, _ := self.sched.GetJobToRun(self.ctx, &agro_pb.JobRequest{Max:proto.Int32(1), WorkerId:proto.String(self.workerId)})
    job, _ := job_stream.Recv()
    if job != nil {
        sleep_size = 1
        log.Printf("Found job: %s", job)
        jobchan <- *job
    } else {
      log.Printf("No jobs found")
      if (sleep_size < 20) {
      //  sleep_size += 1
      }    
      time.Sleep(time.Second * time.Duration(sleep_size))
    }
  } 
  close(jobchan)
}

func (self *ForkManager) Start(engine agro_pb.SchedulerClient, files agro_pb.FileStoreClient) {
  go self.watcher(engine, files)
}

func (self *ForkManager) Run(engine agro_pb.SchedulerClient, files agro_pb.FileStoreClient) {
  self.watcher(engine, files)
}

func (self *ForkManager) SetStatusCheck( check_func func(status agro_engine.EngineStatus)) {
  self.check_func = check_func
}

func NewLocalManager(procCount int, workdir string, workerId string) (*ForkManager, error) {
  return &ForkManager{
    procCount:procCount,
    running:true,
    workdir:workdir,
    workerId:workerId,
    ctx:context.Background(),
  }, nil
}