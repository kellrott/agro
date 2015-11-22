
package agro_db

import (
  "agro/proto"
)

type AgroDB interface {
  AddTask(*agro_pb.Task) error
  AddJob(*agro_pb.Job) error
  SearchTasks(*agro_pb.TagArray) chan agro_pb.TaskInfo

  TaskQuery(*agro_pb.State) chan agro_pb.Task
  JobQuery(*agro_pb.State) chan agro_pb.Job
  
  GetTaskJobs(taskID string) chan agro_pb.Job

  GetJob(jobID string) *agro_pb.Job
  GetJobState(jobID string) *agro_pb.State
  GetTaskStatus(string) agro_pb.TaskStatus
  UpdateTaskState(string, agro_pb.State)
  UpdateJobState(string, agro_pb.State)
  
  SetJobLogs(jobID string,stdout []byte,stderr []byte)

  CreateFile(agro_pb.FileInfo)
  WriteFile(agro_pb.DataBlock)
  CommitFile(agro_pb.FileID)
}