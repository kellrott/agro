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
        sched = agro_pb2.beta_create_Agro_stub(channel)
        filestore = agro_pb2.beta_create_FileStore_stub(channel)
        
        task = agro_pb2.Task()
        task_id = str(uuid.uuid4())
        task.ID = task_id
        task.Command = "/usr/bin/md5sum"
        task.Container = "ubuntu"
        file_id = str(uuid.uuid4())
        
        pyagro.upload_file(filestore, file_id, os.path.join(BASE_DIR, "..", "README"))
        
        task.Args.add( FileArg=agro_pb2.FileArgument(
            ID=file_id, 
            Input=True, 
            Silent=False,
            Type=agro_pb2.FileArgument.PATH),
        ) 
        output_uuid = str(uuid.uuid4())
        task.Args.add( FileArg=agro_pb2.FileArgument(
            ID=output_uuid, 
            Input=False,
            Silent=False,
            Type=agro_pb2.FileArgument.STDOUT),
        ) 
        task.Tags.extend( ['testing'] )
        print "Adding task"
        sched.AddTask(task, 10)

        while True:
            status_list = list(sched.GetTaskStatus(agro_pb2.IDQuery(IDs=[task_id]), 10))
            print status_list
            if sum(list( status.State == agro_pb2.OK for status in status_list )) == len(status_list):
                break
            time.sleep(1)
        
        print "Result File", output_uuid
        
        sched = None
        filestore = None
        