"""KV 存储后端共用接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod


class StoreError(Exception):
    """存储后端操作失败。"""

    def __init__(self, message: str, cause: Exception | None = None):
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:
        if self.cause:
            return f"{self.message} (Caused by: {self.cause})"
        return self.message


class BaseKVTable(ABC):
    """单键表接口。"""

    @abstractmethod
    def set(self, key: str, value: dict) -> None: ...

    @abstractmethod
    def get(self, key: str) -> dict | None: ...

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def list_keys(self) -> list[str]: ...

    @abstractmethod
    def list_all(self) -> dict[str, dict]: ...

    @abstractmethod
    def begin_transaction(self) -> None: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def batch_set(self, items: dict[str, dict]) -> None: ...

    @abstractmethod
    def batch_delete(self, keys: list[str]) -> int: ...


class BaseKKVTable(ABC):
    """双键表接口。"""

    @abstractmethod
    def set(self, pkey: str, skey: str, value: dict) -> None: ...

    @abstractmethod
    def get(self, pkey: str, skey: str) -> dict | None: ...

    @abstractmethod
    def delete(self, pkey: str, skey: str) -> bool: ...

    @abstractmethod
    def list_pkeys(self) -> list[str]: ...

    @abstractmethod
    def list_skeys(self, pkey: str) -> list[str]: ...

    @abstractmethod
    def list_all(self) -> dict[str, dict[str, dict]]: ...

    @abstractmethod
    def begin_transaction(self) -> None: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def batch_set(self, items: dict[str, dict[str, dict]]) -> None: ...

    @abstractmethod
    def batch_delete(self, items: list[tuple[str, str]]) -> int: ...


class BaseDB(ABC):
    """数据库级表管理接口。"""

    TABLE_INFO_TABLE = "_table_info"

    @abstractmethod
    def create_kv_table(self, table_name: str) -> None: ...

    @abstractmethod
    def create_kkv_table(self, table_name: str) -> None: ...

    @abstractmethod
    def get_table(self, table_name: str) -> BaseKVTable | BaseKKVTable: ...

    @abstractmethod
    def list_tables(self) -> dict[str, str]: ...

    @abstractmethod
    def drop_table(self, table_name: str) -> None: ...
