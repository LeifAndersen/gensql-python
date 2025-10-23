import subprocess
import sys
import os

from py4j.java_gateway import JavaGateway

__gateway_server = None
__gateway = None

def start_server():
    global __gateway
    global __gateway_server
    if __gateway_server == None:
        __gateway_server = subprocess.Popen(
            ["java", "-jar", ""],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        wait_text="Ready..."
        for line in __gateway_server.stdout:
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
