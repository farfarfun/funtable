"""Storage contracts shared by the KV backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union


class StoreError(Exception):
    """Error raised by a storage backend."""

    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:
        if self.cause:
            return f"{self.message} (Caused by: {self.cause})"
        return self.message


class BaseKVTable(ABC):
    """Single-key table contract."""

    @abstractmethod
    def set(self, key: str, value: Dict) -> None: ...

    @abstractmethod
    def get(self, key: str) -> Optional[Dict]: ...

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def list_keys(self) -> List[str]: ...

    @abstractmethod
    def list_all(self) -> Dict[str, Dict]: ...

    @abstractmethod
    def begin_transaction(self) -> None: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def batch_set(self, items: Dict[str, Dict]) -> None: ...

    @abstractmethod
    def batch_delete(self, keys: List[str]) -> int: ...


class BaseKKVTable(ABC):
    """Two-key table contract."""

    @abstractmethod
    def set(self, pkey: str, skey: str, value: Dict) -> None: ...

    @abstractmethod
    def get(self, pkey: str, skey: str) -> Optional[Dict]: ...

    @abstractmethod
    def delete(self, pkey: str, skey: str) -> bool: ...

    @abstractmethod
    def list_pkeys(self) -> List[str]: ...

    @abstractmethod
    def list_skeys(self, pkey: str) -> List[str]: ...

    @abstractmethod
    def list_all(self) -> Dict[str, Dict[str, Dict]]: ...

    @abstractmethod
    def begin_transaction(self) -> None: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def batch_set(self, items: Dict[str, Dict[str, Dict]]) -> None: ...

    @abstractmethod
    def batch_delete(self, items: List[tuple[str, str]]) -> int: ...


class BaseDB(ABC):
    """Database-level table management contract."""

    TABLE_INFO_TABLE = "_table_info"

    @abstractmethod
    def create_kv_table(self, table_name: str) -> None: ...

    @abstractmethod
    def create_kkv_table(self, table_name: str) -> None: ...

    @abstractmethod
    def get_table(self, table_name: str) -> Union[BaseKVTable, BaseKKVTable]: ...

    @abstractmethod
    def list_tables(self) -> Dict[str, str]: ...

    @abstractmethod
    def drop_table(self, table_name: str) -> None: ...
