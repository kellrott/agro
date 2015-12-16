#!/usr/bin/env python

import unittest
import uuid
import utilities
import time

from grpc.beta import implementations
from pyagro import agro_pb2
import pyagro

class TestAgroClient(utilities.ServerTest):

    def test_connect(self):
        channel = implementations.insecure_channel('localhost', 9713)
        print "Connected"
        stub = agro_pb2.beta_create_Scheduler_stub(channel)
        
        task_ids = []
        for i in range(5):
            task = agro_pb2.Task()
            task_id = str(uuid.uuid4())
            task.id = task_id
            task_ids.append(task_id)
            task.command = "/bin/echo"
            
            task.args.add( arg="Testing" )
            task.args.add( arg="Hello" )
            task.args.add( arg="World" )
            task.args.add( arg="%s" % (i) )            
            task.container = "ubuntu"
            task.tags.extend( ['testing'] )
            print "Adding task"
            stub.AddTask(task, 10)
        
        for a in stub.SearchTasks(agro_pb2.TagArray(tags=[]), 10):
            print "Found", a
        
        count = 0
        c = pyagro.wait(stub, task_ids)
        assert(c == 0)
        print "Quiting"
        channel = None
        stub = None
        
        #import pdb; pdb.set_trace()
        