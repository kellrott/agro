
import os
import time
from pyagro import agro_pb2

BLOCK_SIZE = 10485760 #10MB
TIMEOUT = 10

def upload_file(agro, file_id, file_path):
    with open(file_path) as handle:
        finfo  = agro_pb2.FileInfo(
            name=os.path.basename(file_path),
            id=file_id,
        )
        agro.CreateFile(finfo, TIMEOUT) 
        pos = 0
        while True:
            data = handle.read(BLOCK_SIZE)
            if not data:
                break
            packet = agro_pb2.DataBlock(
               id=file_id,
               start=pos,
               len=len(data),
               data=data,
            )
            agro.WriteFile(packet, TIMEOUT)
            pos += len(data)
        agro.CommitFile( agro_pb2.FileID(id=file_id), TIMEOUT )

             
def wait(sched, task_ids):
    while True:
        if isinstance(task_ids, basestring):
            task_ids = [task_ids]
        status_list = list(sched.GetTaskStatus(agro_pb2.IDQuery(ids=task_ids), 10))
        print status_list, list( status.state for status in status_list )
        if sum(list( status.state == agro_pb2.OK for status in status_list )) == len(status_list):
            return 0
        if sum(list( status.state == agro_pb2.ERROR for status in status_list )) > 0:
            return 1
        time.sleep(1)
