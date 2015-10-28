package main

import (
  //mesos_exec  "github.com/mesos/mesos-go/executor"
  mesos_proto "github.com/mesos/mesos-go/mesosproto"
  mesos_exec "github.com/mesos/mesos-go/executor"
  "fmt"
  "flag"
  "os"
  "os/exec"
  "encoding/json"
)

type NebulaExecutor struct {
  tasksLaunched int
}


// NewNebulaExecutor returns a mocked executor.
func NewNebulaExecutor() *NebulaExecutor {
	return &NebulaExecutor{
    tasksLaunched : 0,
  }
}

// Registered implements the Registered handler.
func (self *NebulaExecutor) Registered(mesos_exec.ExecutorDriver, *mesos_proto.ExecutorInfo, *mesos_proto.FrameworkInfo, *mesos_proto.SlaveInfo) {
  fmt.Println("Registered")
}

// Reregistered implements the Reregistered handler.
func (self *NebulaExecutor) Reregistered(mesos_exec.ExecutorDriver, *mesos_proto.SlaveInfo) {
  fmt.Println("ReRegistered")
}

// Disconnected implements the Disconnected handler.
func (self *NebulaExecutor) Disconnected(mesos_exec.ExecutorDriver) {
  fmt.Println("Disconnected")
}

// LaunchTask implements the LaunchTask handler.
func (self *NebulaExecutor) LaunchTask(driver mesos_exec.ExecutorDriver, taskInfo *mesos_proto.TaskInfo) {
  fmt.Println("LaunchTask")
  runStatus := &mesos_proto.TaskStatus{
    TaskId: taskInfo.GetTaskId(),
    State:  mesos_proto.TaskState_TASK_RUNNING.Enum(),
  }
  _, err := driver.SendStatusUpdate(runStatus)
  if err != nil {
    fmt.Println("Got error", err)
  }

  self.tasksLaunched++
  fmt.Println("Total tasks launched ", self.tasksLaunched)
  
  go func (task *mesos_proto.TaskInfo) {
    //
    // this is where one would perform the requested task
    //
    task_data_json := task.GetData()
    fmt.Println("Received: %s", string(task_data_json))
    task_data := make(map[string]interface{})
    json.Unmarshal(task_data_json, &task_data)
    cmd_value := task_data["command_line"].(string)
    fmt.Printf("Running: %s\n", cmd_value)
    out, err := exec.Command("/bin/bash", "-c", cmd_value).Output()
    fmt.Print(string(out))
    // finish task
    fmt.Println("Finishing task", task.GetName())
    finStatus := &mesos_proto.TaskStatus{
      TaskId: task.GetTaskId(),
      State:  mesos_proto.TaskState_TASK_FINISHED.Enum(),
    }
    _, err = driver.SendStatusUpdate(finStatus)
    if err != nil {
      fmt.Println("Got error", err)
    }
  } (taskInfo) 

}

// KillTask implements the KillTask handler.
func (self *NebulaExecutor) KillTask(mesos_exec.ExecutorDriver, *mesos_proto.TaskID) {
  fmt.Println("KillTask")
}

// FrameworkMessage implements the FrameworkMessage handler.
func (self *NebulaExecutor) FrameworkMessage(mesos_exec.ExecutorDriver, string) {
  fmt.Println("FrameworkMessage")
}

// Shutdown implements the Shutdown handler.
func (self *NebulaExecutor) Shutdown(driver mesos_exec.ExecutorDriver) {
  fmt.Println("Shutdown")
  driver.Stop()
}

// Error implements the Error handler.
func (self *NebulaExecutor) Error(mesos_exec.ExecutorDriver, string) {
  fmt.Println("Error")
}
func main() {
  fmt.Println(os.Args)
  fmt.Println("Parsing")
  flag.Parse()
  fmt.Println("Init")  
  exec := NewNebulaExecutor()
  config := mesos_exec.DriverConfig {
    Executor: exec,
  }
  fmt.Println("Create")
  driver, err := mesos_exec.NewMesosExecutorDriver(config)
  
	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	_, err = driver.Join()
	if err != nil {
		fmt.Println("driver failed:", err)
	}
	fmt.Println("executor terminating")
  
}