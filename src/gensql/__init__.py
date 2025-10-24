import os
from importlib import resources

from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway

__gateway = None

def start_server():
    global __gateway
    if not __gateway:
        with resources.path(__package__, "gateway.jar") as gateway_jar:
            port = launch_gateway(
                classpath=str(gateway_jar),
                die_on_exit=True
            )
        __gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))

def slurpDB(path):
    start_server()
    p = os.path.abspath(path)
    return __gateway.entry_point.slurpDB(p)

def query(text, db):
    start_server()
    data = __gateway.entry_point.query(text, db)
    return [dict(x) for x in data]

def queryStrict(text, db):
    start_server()
    data = __gateway.entry_point.queryStrict(text, db)
    return [dict(x) for x in data]

def main():
    start_server()

if __name__ == 'main':
    main()
