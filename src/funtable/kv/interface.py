"""KV 存储后端共用接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod


class StoreError(Exception):
    """存储后端操作失败。"""

    def __init__(self, message: str, cause: Exception | None = None) -> None:
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
    def set(self, key: str, value: dict) -> None:
        """将字典 ``value`` 写入 ``key``；写入失败时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def get(self, key: str) -> dict | None:
        """返回 ``key`` 对应的字典；键不存在时返回 ``None``。"""
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        """删除 ``key``，并返回是否删除了已有数据。"""
        ...

    @abstractmethod
    def list_keys(self) -> list[str]:
        """返回表中的全部键。"""
        ...

    @abstractmethod
    def list_all(self) -> dict[str, dict]:
        """返回以键为索引的全部字典值。"""
        ...

    @abstractmethod
    def begin_transaction(self) -> None:
        """开始事务；后端不支持或事务已开始时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def commit(self) -> None:
        """提交当前事务；没有活动事务时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def rollback(self) -> None:
        """回滚当前事务；没有活动事务时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def batch_set(self, items: dict[str, dict]) -> None:
        """批量写入 ``键 -> 字典值`` 映射。"""
        ...

    @abstractmethod
    def batch_delete(self, keys: list[str]) -> int:
        """批量删除指定键，并返回实际删除数量。"""
        ...


class BaseKKVTable(ABC):
    """双键表接口。"""

    @abstractmethod
    def set(self, pkey: str, skey: str, value: dict) -> None:
        """将字典写入主键 ``pkey`` 和次键 ``skey`` 指定的位置。"""
        ...

    @abstractmethod
    def get(self, pkey: str, skey: str) -> dict | None:
        """返回两级键对应的字典；数据不存在时返回 ``None``。"""
        ...

    @abstractmethod
    def delete(self, pkey: str, skey: str) -> bool:
        """删除两级键对应的数据，并返回是否删除了已有数据。"""
        ...

    @abstractmethod
    def list_pkeys(self) -> list[str]:
        """返回全部主键。"""
        ...

    @abstractmethod
    def list_skeys(self, pkey: str) -> list[str]:
        """返回主键 ``pkey`` 下的全部次键。"""
        ...

    @abstractmethod
    def list_all(self) -> dict[str, dict[str, dict]]:
        """返回按主键和次键组织的全部字典值。"""
        ...

    @abstractmethod
    def begin_transaction(self) -> None:
        """开始事务；后端不支持或事务已开始时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def commit(self) -> None:
        """提交当前事务；没有活动事务时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def rollback(self) -> None:
        """回滚当前事务；没有活动事务时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def batch_set(self, items: dict[str, dict[str, dict]]) -> None:
        """批量写入按主键和次键组织的字典值。"""
        ...

    @abstractmethod
    def batch_delete(self, items: list[tuple[str, str]]) -> int:
        """批量删除主键、次键对，并返回实际删除数量。"""
        ...


class BaseDB(ABC):
    """数据库级表管理接口。"""

    TABLE_INFO_TABLE = "_table_info"

    @abstractmethod
    def create_kv_table(self, table_name: str) -> None:
        """创建名为 ``table_name`` 的单键表。"""
        ...

    @abstractmethod
    def create_kkv_table(self, table_name: str) -> None:
        """创建名为 ``table_name`` 的双键表。"""
        ...

    @abstractmethod
    def get_table(self, table_name: str) -> BaseKVTable | BaseKKVTable:
        """返回指定表的操作对象；表不存在时抛出 ``StoreError``。"""
        ...

    @abstractmethod
    def list_tables(self) -> dict[str, str]:
        """返回 ``表名 -> 表类型`` 映射。"""
        ...

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """删除指定表；表不存在时抛出 ``StoreError``。"""
        ...
