
package agro_db

import (
  "agro/proto"
)

type AgroDB interface {
  AddTask(*agro_pb.Task) error
  AddJob(*agro_pb.Job) error
  SearchTasks(*agro_pb.TagArray) chan agro_pb.Task

  TaskQuery(*agro_pb.State) chan agro_pb.Task
  JobQuery(state *agro_pb.State, max int) chan agro_pb.Job
  
  GetTaskJobs(taskID string) chan agro_pb.Job

  GetJob(jobID string) *agro_pb.Job
  GetJobState(jobID string) *agro_pb.State
  GetTaskStatus(string) agro_pb.TaskStatus
  UpdateTaskState(string, agro_pb.State)
  UpdateJobState(string, agro_pb.State)
  
  SetJobLogs(jobID string,stdout []byte,stderr []byte)

  CreateFile(agro_pb.FileInfo) agro_pb.FileState
  WriteFile(agro_pb.DataBlock) agro_pb.FileState
  CommitFile(agro_pb.FileID) agro_pb.FileState
  GetFileInfo(agro_pb.FileID) agro_pb.FileInfo
  ReadFile(req agro_pb.ReadRequest) agro_pb.DataBlock
}