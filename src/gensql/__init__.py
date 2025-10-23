import subprocess
import sys
import os
from importlib import resources

from py4j.java_gateway import JavaGateway

__gateway_server = None
__gateway = None

def start_server():
    global __gateway
    global __gateway_server
    if __gateway_server == None:
        with resources.path(__package__, "gateway.jar") as gateway_jar:
            __gateway_server = subprocess.Popen(
                ["java", "-jar", gateway_jar],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
        wait_text="Running..."
        for line in __gateway_server.stdout:
            print(line)
            if wait_text in line:
                break
        __gateway = JavaGateway()

def slurpDB(path):
    start_server()
    p = os.path.abspath(path)
    return __gateway.entry_point.slurpDB(p)

def query(text, db):
    data = __gateway.entry_point.query(text, db)
    return [dict(x) for x in data]

def queryStrict(text, db):
    data = __gateway.entry_point.queryStrict(text, db)
    return [dict(x) for x in data]

def main():
    start_server

if __name__ == 'main':
    main()
