package agro_mesos

import (
	//"encoding/base64"
	//"flag"
	//"fmt"
	//"strconv"
	//"time"
  "encoding/json"
  "log"
  "fmt"
  "agro/proto"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
	util "github.com/mesos/mesos-go/mesosutil"
)

const (
  FRAMEWORK_ID = "agro"
)

func Inet_itoa(a uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", byte(a), byte(a>>8), byte(a>>16), byte(a>>24))
}

type MesosManager struct {
  readyJobs map[string]*agro_pb.Job
  runningJobs map[string]*agro_pb.Job
  computeCount int
  executor *mesos.ExecutorInfo
}

func NewMesosManager() (*MesosManager, error) {
	return &MesosManager{
      readyJobs:make(map[string]*agro_pb.Job),
      runningJobs:make(map[string]*agro_pb.Job),
      computeCount:0,
  }, nil
}


/*
Code for the Agro JobManager interface
*/
func (self *MesosManager) GetReadyJobCount() int {
  return len(self.readyJobs)
}

func (self *MesosManager) GetRunningJobCount() int {
  return len(self.runningJobs)
}

func (self *MesosManager) GetComputeCount() int {
  return self.computeCount
}

func (self *MesosManager) AddJob(job agro_pb.Job) {
  self.readyJobs[*job.Info.ID] = &job
}

func (self *MesosManager) GetJobState(jobID string) *agro_pb.State {
  if _, ok := self.readyJobs[jobID]; ok {
    out :=  agro_pb.State_QUEUED
    return &out
  }
  return nil
}

func (self *MesosManager) GetReadyJob() *agro_pb.Job {  
  for id, job := range self.readyJobs {
    delete(self.readyJobs, id)
    self.runningJobs[id] = job
    return job
  }  
  return nil
}

/*

*/

func (self *MesosManager) BuildTaskInfo(job *agro_pb.Job, offer *mesos.Offer) *mesos.TaskInfo {
  task_data, _ := json.Marshal( map[string]string{ "command_line" : *job.Info.CommandLine } )
  
  return &mesos.TaskInfo{
    Name: job.Info.ID,
    TaskId:  &mesos.TaskID{
				Value: job.Info.ID,
		},
    SlaveId: offer.SlaveId,
    Executor: self.executor,
    Resources: []*mesos.Resource {
      util.NewScalarResource("cpus", 1),
			util.NewScalarResource("mem", 1024),
    },
    Data:task_data,
  }
}

func (self *MesosManager) BuildExecutorInfo() *mesos.ExecutorInfo  {
  
  executorCommand := fmt.Sprintf("/Users/ellrott/workspaces/agro/agro_exec")
  
  return &mesos.ExecutorInfo {
		ExecutorId:  &mesos.ExecutorID{
      Value: proto.String("AgroExec"),
    },
		Name:       proto.String("AgroExecutor"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String(executorCommand),
			Uris:  []*mesos.CommandInfo_URI{},
		},
	}
}

/*
Code to work with MesosScheduler interface
*/

func (self *MesosManager) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
  log.Printf("ResourceOffers\n")
  computeCount := 0
  for _, offer := range offers {
    offer_cpus := 0
    for _, res := range offer.Resources {
      if *res.Name == "cpus" {
        computeCount += int(*res.Scalar.Value)
        offer_cpus += int(*res.Scalar.Value)
      }
    }

    tasks := make([]*mesos.TaskInfo, 0, computeCount)
    
    for offer_cpus_taken := 0; offer_cpus_taken < offer_cpus; {
        job := self.GetReadyJob()
        if job != nil {
          log.Printf("Launch job: %s", job)
          mesos_taskinfo := self.BuildTaskInfo(job, offer)
          log.Printf("MesosTask: %s", mesos_taskinfo)
          tasks = append(tasks, mesos_taskinfo)
          offer_cpus_taken += 1
        } else {
          offer_cpus_taken = offer_cpus
        }
    }
    if (len(tasks) > 0) {
      _, err := driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{})
      if err != nil {
        fmt.Print("Error: %s", err)
      }
    } else {
      driver.DeclineOffer(offer.Id, &mesos.Filters{})
    }
  }
  self.computeCount = computeCount
  log.Printf("JobsReady:%d JobsRunning:%d CPUsOffered: %d", self.GetReadyJobCount(), self.GetRunningJobCount(), self.computeCount)
}

func (self *MesosManager) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
  log.Printf("StatusUpdate")
}

func (self *MesosManager) Error(driver sched.SchedulerDriver, err string) {
  log.Printf("Error")
}

func (self *MesosManager) ExecutorLost(driver sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Printf("ExecutorLost")
}

func (self *MesosManager) SlaveLost(driver sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Printf("SlaveLost")
}


func (sched *MesosManager) FrameworkMessage(driver sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Printf("framework message from executor %q slave %q: %q", eid, sid, msg)
}


func (sched *MesosManager) OfferRescinded(driver sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Printf("offer rescinded: %v", oid)
}

func (self *MesosManager) Disconnected(driver sched.SchedulerDriver) {
	log.Printf("Disconnected")
}

func (self *MesosManager) Registered(driver sched.SchedulerDriver, fid *mesos.FrameworkID, mi *mesos.MasterInfo) {
	log.Printf("OnRegisterd master:%v:%v, frameworkId:%v", Inet_itoa(mi.GetIp()), mi.GetPort(), fid.GetValue())
	
}

func (self *MesosManager) Reregistered(driver sched.SchedulerDriver, mi *mesos.MasterInfo) {
	log.Printf("OnReregisterd master:%v:%v", Inet_itoa(mi.GetIp()), mi.GetPort())	
}

func (self *MesosManager) Run(master string) {
  
  self.executor = self.BuildExecutorInfo()
  
	//frameworkIdStr := FRAMEWORK_ID
  failoverTimeout := 0.0
	//frameworkId := &mesos.FrameworkID{Value: &frameworkIdStr}
	config := sched.DriverConfig{
		Master: master,
		Framework: &mesos.FrameworkInfo{
			Name:            proto.String("AgroFramework"),
			User:            proto.String(""),
			FailoverTimeout: &failoverTimeout,
			//Id:              frameworkId,
		},
		Scheduler: self,		
	}
	
	driver, err := sched.NewMesosSchedulerDriver(config)
  if err != nil {
    log.Printf("Driver Error: %s", err)
    panic(err)
  }
	//driver.Init()
	//defer driver.Destroy()
	//go self.EventLoop()

	status, err := driver.Start()
  log.Printf("Driver Status:%s", status)
  if err != nil {
    log.Printf("Mesos Start Error: %s", err)
    panic(err)
  }
	//<-self.exit
	//log.Printf("Mesos Exit")
	//driver.Stop(false)
}
