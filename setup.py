
import os
import subprocess

def go_prep():
    env = dict(os.environ)
    env['GOPATH'] = os.path.dirname(os.path.abspath(__file__))

    cmd = "go get github.com/go-python/gopy"
    subprocess.check_call(cmd, shell=True, env=env)

    cmd="go build github.com/go-python/gopy"
    subprocess.check_call(cmd, shell=True, env=env)

    cmd="./gopy bind pywrapper"
    subprocess.check_call(cmd, shell=True, env=env)


if __name__ == "__main__":
    go_prep()
