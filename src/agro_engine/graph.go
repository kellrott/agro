package agro_engine

//import "fmt"

type TaskState int

const (
	WAITING  TaskState = iota
	READY
	RUNNING
	ERROR
	DONE
)

type WorkflowInputs map[string] interface {}

type Task struct {
	Id        string
	Label     string
	DependsOn map[string]*Task
}

type TaskInstance struct {
	State TaskState
	Task  *Task
}

type Workflow struct {
	Id       string
    Tasks    map[string]*Task
	Engine   *Engine
}

type WorkflowInstance struct {
    Id       string
    Inputs   WorkflowInputs
	Workflow *Workflow
	Tasks    map[string]*TaskInstance
}

type Engine struct {
	Workflows map[string]*Workflow
	Instances map[string]*WorkflowInstance
}

func (this *Workflow) NewTask(id string) *Task {
	task := &Task{Id:id, DependsOn:make(map[string]*Task)}
	this.Tasks[id] = task
	return task
}

func (this *Engine) NewWorkflow(id string) *Workflow {
	w := &Workflow{Id:id,Tasks:make(map[string]*Task),Engine:this}
	this.Workflows[id] = w
	return w
}

func NewEngine() *Engine {
	return &Engine{
		Workflows:make(map[string]*Workflow),
		Instances:make(map[string]*WorkflowInstance),
	}
}

/*
func (w *Workflow) AddTask(task *Task) error {
	return nil
}
*/

func (w *Task) AddDepends(task *Task) error {
	w.DependsOn[task.Id] = task
	return nil
}

func (this *TaskInstance) SetDone() {
	this.State = DONE
}

func (this *Workflow) NewInstance(instanceID string, inputs WorkflowInputs) WorkflowInstance {
	n := WorkflowInstance{
		Id : instanceID,
		Inputs : inputs,
		Workflow:this,
		Tasks:make(map[string]*TaskInstance),
	}
	for taskName, task := range(this.Tasks) {
		n.Tasks[taskName] = &TaskInstance{ State:WAITING, Task:task }
	}
	this.Engine.Instances[instanceID] = &n
	return n
}

func (this *WorkflowInstance) updateStates() {
	sync := make(chan bool)
	change := true
	for change {
		for _, task := range this.Tasks {
			go func(curTask *TaskInstance) {
				taskChange := false
				if curTask.State == WAITING {
					ready := true
					for dependsName := range curTask.Task.DependsOn {
						switch this.Tasks[dependsName].State {
						case WAITING:
							ready = false
						case READY:
							ready = false
						case RUNNING:
							ready = false
						case ERROR:
							ready = false
							curTask.State = ERROR
							taskChange = true
						}
					}
					if ready {
						curTask.State = READY
						taskChange =  true
					}
				}
				sync <- taskChange
			}(task)
		}
		change = false
		for range this.Tasks {
			if ( <- sync ) {
				change = true
			}
		}
	}
}

func (this *Engine) Scan() chan *TaskInstance {
	for _, inst := range(this.Instances) {
		inst.updateStates()
	}

	out := make(chan *TaskInstance, 100)
	go func() {
		for _, inst := range(this.Instances) {
			for _, taskInstance := range(inst.Tasks) {
				if taskInstance.State == READY {
					out <- taskInstance
				}
			}
		}
		close(out)
	} ()
	return out
}
