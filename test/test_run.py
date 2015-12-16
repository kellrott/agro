#!/usr/bin/env python

import os
import unittest
import uuid
import utilities
import time

from grpc.beta import implementations

import pyagro
from pyagro import agro_pb2

BASE_DIR = os.path.dirname(__file__)

class TestAgroClient(utilities.ServerTest):

    def test_program(self):
        channel = implementations.insecure_channel('localhost', 9713)
        print "Connected"
        sched = agro_pb2.beta_create_Scheduler_stub(channel)
        filestore = agro_pb2.beta_create_FileStore_stub(channel)
        
        task = agro_pb2.Task()
        task_id = str(uuid.uuid4())
        task.id = task_id
        task.command = "/usr/bin/md5sum"
        task.container = "ubuntu"
        file_id = str(uuid.uuid4())
        
        pyagro.upload_file(filestore, file_id, os.path.join(BASE_DIR, "..", "README"))
        
        task.args.add( file_arg=agro_pb2.FileArgument(
            id=file_id, 
            input=True, 
            silent=False,
            type=agro_pb2.FileArgument.PATH),
        )
        output_uuid = str(uuid.uuid4())
        task.args.add( file_arg=agro_pb2.FileArgument(
            id=output_uuid, 
            input=False,
            silent=False,
            type=agro_pb2.FileArgument.STDOUT),
        ) 
        task.tags.extend( ['testing'] )
        print "Adding task"
        sched.AddTask(task, 10)

        e = pyagro.wait(sched, task_id)
        assert(e == 0)
        print "Result File", output_uuid
        
        sched = None
        filestore = None
        