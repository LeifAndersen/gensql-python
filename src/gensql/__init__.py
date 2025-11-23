"""
GenSQL
======

Python bindings for GenSQL.

This package requires Java (>= 17) to be installed, either in `$JAVA_HOME`, 
or in `$PATH`.
"""

from .db import DB

__all__ = ["DB"]