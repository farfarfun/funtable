from importlib import import_module

from .interface import BaseDB, BaseKKVTable, BaseKVTable, StoreError
from .sqlite_table import SQLiteKKVTable, SQLiteKVTable, SQLiteStore

_TINYDB_EXPORTS = {"TinyDBKKVTable", "TinyDBKVTable", "TinyDBStore"}


def __getattr__(name):
    if name in _TINYDB_EXPORTS:
        return getattr(import_module(".tinydb_table", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BaseDB",
    "BaseKKVTable",
    "BaseKVTable",
    "SQLiteKKVTable",
    "SQLiteKVTable",
    "SQLiteStore",
    "StoreError",
    "TinyDBKKVTable",
    "TinyDBKVTable",
    "TinyDBStore",
]
