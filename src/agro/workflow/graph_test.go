package agro_engine


import (
	"work_engine"
	"testing"
	"fmt"
)


func TestGraphElements(t *testing.T) {

	engine := work_engine.NewEngine()

	workflow := engine.NewWorkflow("workflow-1")
	task_1 := workflow.NewTask("task-1")
	task_2 := workflow.NewTask("task-2")
	task_2.AddDepends(task_1)

	workflow.NewInstance("instance-1",
		work_engine.WorkflowInputs {
			"instance-1" : "test",
		},
	)

	for n := range engine.Scan() {
		fmt.Println("Ready", n.Task.Id)
		n.SetDone()
	}

	for n := range engine.Scan() {
		fmt.Println("Ready", n.Task.Id)
		n.SetDone()
	}

	for n := range engine.Scan() {
		fmt.Println("Ready", n.Task.Id)
		n.SetDone()
	}

}
