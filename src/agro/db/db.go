
package agro_db

import (
  "agro/proto"
)

type AgroDB interface {
  AddTask(*agro_pb.Task) error
  TaskQuery(*agro_pb.State) chan agro_pb.Task
}