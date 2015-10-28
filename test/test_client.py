#!/usr/bin/env python


from grpc.beta import implementations
from pyagro import agro_pb2

if __name__ == "__main__":
    
    task = agro_pb2.Task()
    task.ID = "test"
    task.CommandLine = "/bin/echo Testing"
    task.Container = "ubuntu"

    channel = implementations.insecure_channel('localhost', 9713)
    print "Connected"
    stub = agro_pb2.beta_create_Tasks_stub(channel)
    print "Adding task"
    stub.AddTask(task, 30)
    stub = None
