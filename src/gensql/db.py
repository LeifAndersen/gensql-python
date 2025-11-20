import os
from importlib import resources
from typing import Any

from py4j.java_gateway import JavaGateway

_gateway: Any = None
_entry: Any = None

def start_gateway():
    """
    Start the GenSQL gateway, one per process.

    This gateway is started automatically when a DB is created.

    Current limitation: only one instance of this gateway can be running
    a given machine.
    """
    global _gateway
    global _entry
    if not _gateway:
        if __package__ is None:
            raise RuntimeError("This function must be called from within a package")
        with resources.path(__package__, "gateway.jar") as gateway_jar:
            _gateway = JavaGateway.launch_gateway(
                jarpath=str(gateway_jar),
                die_on_exit=True
            )
            _entry = _gateway.jvm.gensql.gateway.Gateway

class DB:
    """
    A GenSQL Database
    """
    def __init__(self, path: str) -> None:
        """
        Initialize a GenSQL Database

        Parameters:
            path: The path to the db.edn file.
        """
        start_gateway()
        p = os.path.abspath(path)
        self.db = _entry.slurpDB(p)

    def query(self, text: str, mode: str = "permissive") -> list[dict[str, Any]]:
        """
        Query the database.

        Parameters:
            text: The query text.
            mode: The query language. Can be one of:
                - permissive
                - string
        """
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
