
package agro_drmaa

import (
  "github.com/dgruber/drmaa"
  "agro/engine"
  "time"
  "log"
)


type DrmaaManager struct {
  engine *agro_engine.WorkEngine
  session drmaa.Session
  running bool
}


func NewDrmaaManager() (*DrmaaManager, error) {
  return &DrmaaManager{running:false}, nil
}


func (self *DrmaaManager) run() {
  
  for self.running { 
    job := self.engine.GetJobToRun()
    if job != nil {
        log.Printf("Launch job: %s", job)
        //mesos_taskinfo := self.BuildTaskInfo(job, offer)
        //log.Printf("MesosTask: %s", mesos_taskinfo)
        //tasks = append(tasks, mesos_taskinfo)
        //offer_cpus_taken += 1
    } else {
      time.Sleep(time.Second)
    }
  }  
}

func (self *DrmaaManager) Start(engine *agro_engine.WorkEngine) {
  self.engine = engine
  s, _ := drmaa.MakeSession()
  self.session = s  
  self.running = true
  go self.run()
}