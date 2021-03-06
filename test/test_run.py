#!/usr/bin/env python

import os
import unittest
import uuid
import subprocess
from urlparse import urlparse
import utilities
import time

from grpc.beta import implementations

import pyagro
from pyagro import agro_pb2

BASE_DIR = os.path.dirname(__file__)

def get_abspath(path):
    return os.path.join(os.path.dirname(__file__), path)

class TestAgroClient(utilities.ServerTest):

    def setUp(self):
        cmd = "docker-compose -f %s up -d" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)
        
        self.agro_server = "localhost"
        if 'DOCKER_HOST' in os.environ:
            self.agro_server = urlparse(os.environ['DOCKER_HOST']).netloc.split(":")[0]
        
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None

    def tearDown(self):
        return
        cmd = "docker-compose -f %s stop" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)
        cmd = "docker-compose -f %s rm -fv" % (get_abspath("../docker-compose/agro/docker-compose.yml"))
        subprocess.check_call(cmd, shell=True)

    def test_program(self):
        #channel = implementations.insecure_channel('localhost', 9713)
        channel = implementations.insecure_channel(self.agro_server, 9713)
        
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
    
    def test_docker_client(self):
        channel = implementations.insecure_channel(self.agro_server, 9713)
        
        print "Connected"
        sched = agro_pb2.beta_create_Scheduler_stub(channel)
        
        task = agro_pb2.Task()
        task_id = str(uuid.uuid4())
        task.id = task_id
        task.command = "/usr/local/bin/docker"
        task.container = "docker"
        task.requirements.extend( [agro_pb2.TaskRequirement(
            name="docker_socket",
            value="/var/run/docker.sock"
        ) ] )

        task.args.add( arg="images" )
        print "Adding task"
        sched.AddTask(task, 10)
        print "Waiting"
        e = pyagro.wait(sched, task_id)
        assert(e == 0)
        
        sched = None
        filestore = None
        