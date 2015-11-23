
import os
from pyagro import agro_pb2

BLOCK_SIZE = 10485760 #10MB
TIMEOUT = 10

def upload_file(agro, file_id, file_path):
    with open(file_path) as handle:
        finfo  = agro_pb2.FileInfo(
            Name=os.path.basename(file_path),
            ID=file_id,
        )
        agro.CreateFile(finfo, TIMEOUT) 
        pos = 0
        while True:
            data = handle.read(BLOCK_SIZE)
            if not data:
                break
            packet = agro_pb2.DataBlock(
               ID=file_id,
               Start=pos,
               Len=len(data),
               Data=data,
            )
            agro.WriteFile(packet, TIMEOUT)
            pos += len(data)
        agro.CommitFile( agro_pb2.FileID(ID=file_id), TIMEOUT )

             
             