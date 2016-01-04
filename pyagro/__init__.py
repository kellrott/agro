
import os
import time
from copy import copy
from pyagro import agro_pb2
from grpc.beta import implementations

BLOCK_SIZE = 10485760 #10MB
TIMEOUT = 10

class AgroScheduler:
    def __init__(self, stub):
        self.stub = stub
        
class AgroFileStore:
    def __init__(self, stub):
        self.stub = stub

    def __getattr__(self, name):
        if hasattr(self.stub, name):
            f = getattr(self.stub, name)
            def wrapper(*args, **kwargs):
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = 10
                return f(*args, **kwargs)
            return wrapper                
        else:
            raise AttributeError

class AgroClient:
    def __init__(self, addr="localhost:9713"):
        tmp = addr.split(":")
        self.host = tmp[0]
        if len(tmp) > 1:
            self.port = tmp[1]
        else:
            self.port = "9713"
        self.channel = implementations.insecure_channel(self.host, int(self.port))

    def scheduler(self):
        stub = agro_pb2.beta_create_Scheduler_stub(self.channel)
        return AgroScheduler(stub)

    def filestore(self):
        stub = agro_pb2.beta_create_FileStore_stub(self.channel)
        return AgroFileStore(stub)

def scan_value(value, prefix=[]):    
    if isinstance(value, dict):
        yield agro_pb2.Field(path=copy(prefix), map_declare=len(value))
        for k, v in value.items():
            for f in scan_value(v, copy(prefix + [k])):
                yield f
    elif isinstance(value, list):
        yield agro_pb2.Field(path=copy(prefix), list_declare=len(value))
        for i, v in enumerate(value):
            for f in scan_value(v, copy(prefix + [str(i)])):
                yield f
    elif isinstance(value, basestring):
        yield agro_pb2.Field(path=prefix, str_value=value)
    elif isinstance(value, bool):
        yield agro_pb2.Field(path=prefix, bool_value=value)
    elif isinstance(value, int):
        yield agro_pb2.Field(path=prefix, int_value=value)
    elif isinstance(value, float):
        yield agro_pb2.Field(path=prefix, float_value=value)
    elif value is None:
        yield agro_pb2.Field(path=prefix)
    else:
        raise Exception("Unable to pack: %s" % (value))
        

def pack_doc(id, doc):
    fields = list(scan_value(doc))
    #print "outfields: ", len(fields)
    return agro_pb2.Document(id=id, fields=fields)

def populate_obj(data, path, field):
    if len(path) > 1:
        if isinstance(data, list):
            populate_obj(data[int(path[0])], path[1:], field)
        elif isinstance(data, dict):
            #print "populate", data
            populate_obj(data[path[0]], path[1:], field)
    elif len(path) == 1:
        field_type = field.WhichOneof("Value")
        if field_type in ["map_declare", "list_declare"]:
            if field_type == "map_declare":
                v = {}
            elif field_type == "list_declare":
                v = [None] * field.list_declare
            if isinstance(data, list):
                data[int(path[0])] = v             
            else:
                data[path[0]] = v
        else:
            value = None
            if field_type == "bool_value":
                value = field.bool_value
            elif field_type == "str_value":
                value = field.str_value
            elif field_type == "float_value":
                value = field.float_value
            elif field_type == "int_value":
                value = field.int_value
            if isinstance(data, list):
                data[int(path[0])] = value
            else:
                data[path[0]] = value
            

def unpack_doc(doc):
    o = {}
    #print "in fields", len(doc.fields)
    for f in doc.fields:
        populate_obj(o, f.path, f)
    o['_id'] = doc.id
    return o
    

def upload_file(agro, file_id, file_path):
    with open(file_path) as handle:
        finfo  = agro_pb2.FileInfo(
            name=os.path.basename(file_path),
            id=file_id,
        )
        agro.CreateFile(finfo, timeout=TIMEOUT) 
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
            agro.WriteFile(packet, timeout=TIMEOUT)
            pos += len(data)
        agro.CommitFile( agro_pb2.FileID(id=file_id), timeout=TIMEOUT )

def download_file(agro, file_id, file_path):
    with open(file_path, "wb") as handle:
        info = agro.GetFileInfo(agro_pb2.FileID(id=file_id), timeout=TIMEOUT)
        for i in xrange(0, info.size, BLOCK_SIZE):
            block = agro.ReadFile(agro_pb2.ReadRequest(id=file_id, start=i, size=BLOCK_SIZE))
            handle.write(block.data)
             
def wait(sched, task_ids):
    while True:
        if isinstance(task_ids, basestring):
            task_ids = [task_ids]
        status_list = list(sched.GetTaskStatus(agro_pb2.IDQuery(ids=task_ids), 10))
        #print status_list, list( status.state for status in status_list )
        if sum(list( status.state == agro_pb2.OK for status in status_list )) == len(status_list):
            return 0
        if sum(list( status.state == agro_pb2.ERROR for status in status_list )) > 0:
            return 1
        time.sleep(1)
