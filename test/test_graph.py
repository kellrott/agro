
import pyagro
import unittest
import time
import os
import shutil

class TestGraphOps(unittest.TestCase):

	engine = pyagro.NewEngine()

	workflow = engine.NewWorkflow("workflow-1")
	task_1 = workflow.NewTask("task-1")
	task_2 = workflow.NewTask("task-2")
	task_2.AddDepends(task_1)

	workflow.NewInstance("instance-1",
		{}
	)

	for n in engine.Scan():
		print("Ready", n.Task.Id)
		n.SetDone()

	for n in engine.Scan():
		print("Ready", n.Task.Id)
		n.SetDone()

	for n := range engine.Scan() {
		print("Ready", n.Task.Id)
		n.SetDone()
