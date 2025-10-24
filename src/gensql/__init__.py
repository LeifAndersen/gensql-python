import os
from importlib import resources

from py4j.java_gateway import JavaGateway

__gateway = None
__entry = None

def start_gateway():
    global __gateway
    global __entry
    if not __gateway:
        with resources.path(__package__, "gateway.jar") as gateway_jar:
            __gateway = JavaGateway.launch_gateway(
                jarpath=str(gateway_jar),
                die_on_exit=True
            )
            __entry = __gateway.jvm.gensql.gateway.Gateway

def slurpDB(path):
    start_gateway()
    p = os.path.abspath(path)
    return __entry.slurpDB(p)

def query(text, db):
    start_gateway()
    data = __entry.query(text, db)
    return [dict(x) for x in data]

def queryStrict(text, db):
    start_gateway()
    data = __entry.queryStrict(text, db)
    return [dict(x) for x in data]

def main():
    start_gateway()

if __name__ == 'main':
    main()
