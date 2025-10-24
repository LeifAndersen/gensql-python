import os
from importlib import resources
from typing import Any

from py4j.java_gateway import JavaGateway

_gateway = None
_entry = None

def start_gateway():
    global _gateway
    global _entry
    if not _gateway:
        with resources.path(__package__, "gateway.jar") as gateway_jar:
            _gateway = JavaGateway.launch_gateway(
                jarpath=str(gateway_jar),
                die_on_exit=True
            )
            _entry = _gateway.jvm.gensql.gateway.Gateway

class DB:
    def __init__(self, path: str) -> None:
        start_gateway()
        p = os.path.abspath(path)
        self.db = _entry.slurpDB(p)

    def query(self, text: str, mode: str = "permissive") -> list[dict[str, Any]]:
        if mode == "permissive":
            return self.queryPermissive(text)
        elif mode == "strict":
            return self.queryStrict(text)
        else:
            raise ValueError("Invalid mode", mode)

    def queryPermissive(self, text: str) -> list[dict[str, Any]]:
        data = _entry.query(text, self.db)
        return [dict(x) for x in data]

    def queryStrict(self, text: str) -> list[dict[str, Any]]:
        data = _entry.queryStrict(text, self.db)
        return [dict(x) for x in data]

def main():
    start_gateway()

if __name__ == 'main':
    main()
