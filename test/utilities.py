
import unittest
import os
from urlparse import urlparse
import logging
import time
import socket
import subprocess

SETUP_MONGO = True
SETUP_AGRO = True
MONGO_IMAGE = "mongo"
MONGO_NAME = "agro_mongo"
CONFIG_SUDO = False
WORK_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "test_work" )
TEST_MONGO="localhost"

MONGO_PORT="27017"

def which(file):
    for path in os.environ["PATH"].split(":"):
        p = os.path.join(path, file)
        if os.path.exists(p):
            return p


def get_host_ip():
    s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    h = s.getsockname()
    s.close()
    return h[0]
    
def get_docker_path():
    docker_path = which('docker')
    if docker_path is None:
        raise Exception("Cannot find docker")
    return docker_path
    
def call_docker_run(
    image, ports={},
    args=[], host=None, sudo=False,
    env={},
    set_user=False,
    mounts={},
    links={},
    privledged=False,net=None,
    name=None):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "run"
    ]

    if set_user:
        cmd.extend( ["-u", str(os.geteuid())] )
    for k, v in ports.items():
        cmd.extend( ["-p", "%s:%s" % (k,v) ] )
    for k, v in env.items():
        cmd.extend( ["-e", "%s=%s" % (k,v)] )
    if name is not None:
        cmd.extend( ["--name", name])
    for k, v in mounts.items():
        cmd.extend( ["-v", "%s:%s" % (k, v)])
    if privledged:
        cmd.append("--privileged")
    if net is not None:
        cmd.extend(["--net", net])
    for k,v in links.items():
        cmd.extend( ["--link", "%s:%s" % (k,v)] )
    cmd.append("-d")
    cmd.extend( [image] )
    cmd.extend(args)

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))


def call_docker_kill(
    name,
    host=None, sudo=False
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "kill", name
    ]
    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    subprocess.check_call(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)


def call_docker_rm(
    name=None, volume_delete=False,
    host=None, sudo=False
    ):

    docker_path = get_docker_path()

    cmd = [
        docker_path, "rm"
    ]
    if volume_delete:
        cmd.append("-v")
    cmd.append(name)

    sys_env = dict(os.environ)
    if host is not None:
        sys_env['DOCKER_HOST'] = host
    if sudo:
        cmd = ['sudo'] + cmd
    logging.info("executing: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, close_fds=True, env=sys_env, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("Call Failed: %s" % (cmd))


class ServerTest(unittest.TestCase):

    def setUp(self):
        if not os.path.exists("./test_tmp"):
            os.mkdir("test_tmp")
        self.service = None
        self.mongo_url = TEST_MONGO
        if SETUP_MONGO:
            if 'DOCKER_HOST' in os.environ:
                n = urlparse(os.environ['DOCKER_HOST'])
                self.host_ip = n.netloc.split(":")[0]
            else:
                self.host_ip = get_host_ip()
            logging.info("Using HostIP: %s" % (self.host_ip))

            call_docker_run(image=MONGO_IMAGE,
                ports={MONGO_PORT:MONGO_PORT},
                sudo=CONFIG_SUDO,
                name=MONGO_NAME,
            )
            time.sleep(5) 
            self.mongo_url = self.host_ip

        if SETUP_AGRO:
            cmd = ["./bin/agro-manager", "--mongo", self.mongo_url]
            logging.info("Running %s" % (" ".join(cmd)))
            self.agro_server = subprocess.Popen(cmd)
            time.sleep(3)        
            cmd = ["./bin/agro-worker", "--workdir", WORK_DIR]
            logging.info("Running %s" % (" ".join(cmd)))
            self.agro_server = subprocess.Popen(cmd)
            

    def tearDown(self):
        if self.service is not None:
            self.service.stop()
            self.service = None
            time.sleep(5)

        if SETUP_MONGO:
            call_docker_kill(MONGO_NAME)
            call_docker_rm(MONGO_NAME, volume_delete=True)
        
        if SETUP_AGRO:
            self.agro_server.kill()
