"""
TinyDB存储实现模块

基于TinyDB实现KV和KKV存储接口。
TinyDB是一个轻量级的文档型数据库，数据以JSON格式存储。
"""

from __future__ import annotations

import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from farlog import getLogger
from tinydb import Query, TinyDB
from tinydb.table import Table

from .interface import (
    BaseDB,
    BaseKKVTable,
    BaseKVTable,
    StoreError,
)

logger = getLogger("funtable")

_TINYDB_OPERATION_ERRORS = (OSError, TypeError, ValueError)


class TinyDBTableBase:
    """TinyDB表基类"""

    _db_instances: dict[str, TinyDB] = {}
    # 当前使用进程级锁；如需提高吞吐量，可按数据库路径拆分锁。
    _lock = threading.RLock()

    def __init__(self, db_path: str):
        """初始化TinyDB连接"""
        self.db_path = db_path

    @property
    def db(self) -> TinyDB:
        """获取共享的数据库连接"""
        try:
            with self._lock:
                if self.db_path not in self._db_instances:
                    self._db_instances[self.db_path] = TinyDB(self.db_path)
                return self._db_instances[self.db_path]
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to connect to TinyDB database: {str(e)}")
            raise StoreError(f"连接 TinyDB 数据库失败（路径：{self.db_path}）", cause=e) from e

    @classmethod
    def _close_db(cls, db_path: str) -> None:
        with cls._lock:
            db = cls._db_instances.pop(db_path, None)
            if db is not None:
                db.close()

    def close(self) -> None:
        """关闭数据库连接"""
        try:
            self._close_db(self.db_path)
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error closing database connection: {str(e)}")
            raise StoreError(f"关闭 TinyDB 数据库失败（路径：{self.db_path}）", cause=e) from e

    def __del__(self) -> None:
        """析构函数"""
        try:
            self.close()
        except StoreError as e:
            logger.warning(f"析构时关闭 TinyDB 连接失败，已忽略：{e}")

    def begin_transaction(self) -> None:
        """TinyDB 不支持事务，调用即抛出 ``StoreError``。"""
        raise StoreError("TinyDB does not support transactions")

    def commit(self) -> None:
        """TinyDB 不支持事务，调用即抛出 ``StoreError``。"""
        raise StoreError("TinyDB does not support transactions")

    def rollback(self) -> None:
        """TinyDB 不支持事务，调用即抛出 ``StoreError``。"""
        raise StoreError("TinyDB does not support transactions")

    def _validate_key(self, key: str) -> None:
        """验证键是否有效"""
        if not isinstance(key, str):
            raise StoreError("Key must be string type")
        if not key:
            raise StoreError("Key cannot be empty")
        if len(key) > 128:
            raise StoreError("Key too long (max 128 characters)")

    def _validate_value(self, value: dict) -> None:
        """验证值是否有效"""
        if not isinstance(value, dict):
            raise StoreError("Value must be dictionary type")
        if not value:
            raise StoreError("Value cannot be empty")


class TinyDBKVTable(TinyDBTableBase, BaseKVTable):
    """TinyDB的KV存储实现类

    使用TinyDB实现键值对存储，每个文档格式为:
    {
        "key": "键名",
        "value": {"key1": "value1", ...}  # 值必须是字典类型
    }
    """

    def __init__(self, table_name: str, db_path: str):
        """初始化TinyDB KV表"""
        TinyDBTableBase.__init__(self, db_path)
        self.table_name = table_name
        self.query = Query()

    @property
    def table(self) -> Table:
        """获取表对象"""
        return self.db.table(self.table_name)

    def set(self, key: str, value: dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(key)
            self._validate_value(value)

            with self._lock:
                self.table.upsert(
                    {"key": key, "value": value},
                    self.query.key == key,
                )

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error setting KV pair: {str(e)}")
            raise StoreError(f"设置 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def get(self, key: str) -> dict | None:
        """获取键值对"""
        try:
            self._validate_key(key)

            with self._lock:
                result = cast(
                    dict[str, Any] | None, self.table.get(self.query.key == key)
                )
                return cast(dict[str, Any], result["value"]) if result else None

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error getting value: {str(e)}")
            raise StoreError(f"读取 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def delete(self, key: str) -> bool:
        """删除键值对"""
        try:
            self._validate_key(key)

            with self._lock:
                return len(self.table.remove(self.query.key == key)) > 0

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error deleting KV pair: {str(e)}")
            raise StoreError(f"删除 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def batch_set(self, items: dict[str, dict]) -> None:
        """批量设置键值对"""
        try:
            for key, value in items.items():
                self._validate_key(key)
                self._validate_value(value)
            with self._lock:
                for key, value in items.items():
                    self.table.upsert(
                        {"key": key, "value": value}, self.query.key == key
                    )

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError(f"批量设置 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def batch_delete(self, keys: list[str]) -> int:
        """批量删除键值对"""
        try:
            for key in keys:
                self._validate_key(key)
            with self._lock:
                return len(self.table.remove(self.query.key.one_of(keys)))

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError(f"批量删除 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def list_keys(self) -> list[str]:
        """获取所有键列表

        Returns:
            包含所有键的列表
        """
        with self._lock:
            return [doc["key"] for doc in self.table.all()]

    def list_all(self) -> dict[str, dict]:
        """获取所有键值对数据

        Returns:
            包含所有键值对的字典，格式为 {key: value_dict}
        """
        with self._lock:
            return {doc["key"]: doc["value"] for doc in self.table.all()}


class TinyDBKKVTable(TinyDBTableBase, BaseKKVTable):
    """TinyDB的KKV存储实现类

    使用TinyDB实现两级键的存储，每个文档格式为:
    {
        "key1": "主键名",
        "key2": "次键名",
        "value": {"key1": "value1", ...}  # 值必须是字典类型
    }
    """

    def __init__(self, table_name: str, db_path: str):
        """初始化TinyDB KKV表"""
        TinyDBTableBase.__init__(self, db_path)
        self.table_name = table_name
        self.query = Query()

    @property
    def table(self) -> Table:
        """获取表对象"""
        return self.db.table(self.table_name)

    def set(self, key1: str, key2: str, value: dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(key1)
            self._validate_key(key2)
            self._validate_value(value)

            with self._lock:
                self.table.upsert(
                    {"key1": key1, "key2": key2, "value": value},
                    (self.query.key1 == key1) & (self.query.key2 == key2),
                )

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error setting KKV pair: {str(e)}")
            raise StoreError(f"设置 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def get(self, key1: str, key2: str) -> dict | None:
        """获取键值对"""
        try:
            self._validate_key(key1)
            self._validate_key(key2)

            with self._lock:
                result = cast(
                    dict[str, Any] | None,
                    self.table.get(
                        (self.query.key1 == key1) & (self.query.key2 == key2)
                    ),
                )
                return cast(dict[str, Any], result["value"]) if result else None

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error getting value: {str(e)}")
            raise StoreError(f"读取 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def delete(self, key1: str, key2: str) -> bool:
        """删除键值对"""
        try:
            self._validate_key(key1)
            self._validate_key(key2)

            with self._lock:
                return (
                    len(
                        self.table.remove(
                            (self.query.key1 == key1) & (self.query.key2 == key2)
                        )
                    )
                    > 0
                )

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error deleting KKV pair: {str(e)}")
            raise StoreError(f"删除 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def batch_set(self, items: dict[str, dict[str, dict]]) -> None:
        """批量设置键值对

        Args:
            items: 格式为 {pkey: {skey: value_dict}}
        """
        try:
            for pkey, skey_dict in items.items():
                for skey, value in skey_dict.items():
                    self._validate_key(pkey)
                    self._validate_key(skey)
                    self._validate_value(value)
            with self._lock:
                for pkey, skey_dict in items.items():
                    for skey, value in skey_dict.items():
                        self.table.upsert(
                            {"key1": pkey, "key2": skey, "value": value},
                            (self.query.key1 == pkey) & (self.query.key2 == skey),
                        )

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError(f"批量设置 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def batch_delete(self, items: list[tuple[str, str]]) -> int:
        """批量删除键值对

        Args:
            items: 要删除的键对列表 [(pkey, skey)]
        """
        try:
            deleted = 0
            for pkey, skey in items:
                self._validate_key(pkey)
                self._validate_key(skey)
            with self._lock:
                for pkey, skey in items:
                    deleted += len(
                        self.table.remove(
                            (self.query.key1 == pkey) & (self.query.key2 == skey)
                        )
                    )
            return deleted

        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError(f"批量删除 TinyDB 数据失败（表：{self.table_name}）", cause=e) from e

    def list_pkeys(self) -> list[str]:
        """获取所有第一级键列表"""
        with self._lock:
            return list(set(doc["key1"] for doc in self.table.all()))

    def list_skeys(self, pkey: str) -> list[str]:
        """获取指定第一级键下的所有第二级键列表"""
        self._validate_key(pkey)
        with self._lock:
            return [doc["key2"] for doc in self.table.search(self.query.key1 == pkey)]

    def list_all(self) -> dict[str, dict[str, dict]]:
        """获取所有键值对数据"""
        with self._lock:
            result: dict[str, dict[str, dict]] = {}
            for doc in self.table.all():
                pkey = doc["key1"]
                skey = doc["key2"]
                if pkey not in result:
                    result[pkey] = {}
                result[pkey][skey] = doc["value"]
            return result


class TinyDBStore(TinyDBTableBase, BaseDB):
    """TinyDB数据库维度存储实现"""

    TABLE_INFO_TABLE = "table_info"

    def __init__(self, db_dir: str = "tinydb_store"):
        """初始化TinyDB存储

        Args:
            db_dir: 数据库文件目录
        """
        logger.info(f"Initializing TinyDBStore in directory: {db_dir}")
        try:
            self.db_dir = db_dir
            os.makedirs(db_dir, exist_ok=True)
            self._table_name_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
            self._table_info_path = os.path.join(db_dir, ".table_info")
            super().__init__(self._table_info_path)
            self._init_table_info_table()
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to initialize TinyDBStore: {str(e)}")
            raise StoreError(f"初始化 TinyDB 存储失败（目录：{db_dir}）", cause=e) from e

    def _init_table_info_table(self) -> None:
        """初始化存储表信息表"""
        try:
            with self._lock:
                table = self.db.table(self.TABLE_INFO_TABLE)
                if not table.all():
                    logger.info("Initializing table info storage")
                    table.insert({"created_at": datetime.now().isoformat()})
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to initialize table info: {str(e)}")
            raise StoreError("初始化 TinyDB 表信息失败", cause=e) from e

    def _add_table_info(self, table_name: str, table_type: str) -> None:
        """添加或更新存储表信息"""
        try:
            with self._lock:
                table = self.db.table(self.TABLE_INFO_TABLE)
                table.upsert(
                    {
                        "name": table_name,
                        "type": table_type,
                        "updated_at": datetime.now().isoformat(),
                    },
                    Query().name == table_name,
                )
            logger.info(f"Added/updated table info: {table_name} ({table_type})")
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to add/update table info: {str(e)}")
            raise StoreError(f"更新 TinyDB 表信息失败（表：{table_name}）", cause=e) from e

    def _remove_table_info(self, table_name: str) -> None:
        """删除存储表信息"""
        try:
            with self._lock:
                table = self.db.table(self.TABLE_INFO_TABLE)
                table.remove(Query().name == table_name)
            logger.info(f"Removed table info: {table_name}")
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to remove table info: {str(e)}")
            raise StoreError(f"删除 TinyDB 表信息失败（表：{table_name}）", cause=e) from e

    def _get_table_type(self, table_name: str) -> str:
        """获取存储表类型"""
        try:
            with self._lock:
                table = self.db.table(self.TABLE_INFO_TABLE)
                result = cast(
                    dict[str, Any] | None, table.get(Query().name == table_name)
                )
                if not result:
                    raise StoreError(f"Table not found: {table_name}")
                return str(result["type"])
        except StoreError:
            raise
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to get table type: {str(e)}")
            raise StoreError(f"读取 TinyDB 表类型失败（表：{table_name}）", cause=e) from e

    def _get_db_path(self, table_name: str) -> str:
        """获取存储表的数据库文件路径"""
        return os.path.join(self.db_dir, f"{table_name}.json")

    def _validate_table_name(self, table_name: str) -> None:
        """验证表名是否有效"""
        if not table_name:
            raise StoreError("Table name cannot be empty")
        if len(table_name) > 128:
            raise StoreError("Table name too long (max 128 characters)")
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", table_name):
            raise StoreError(
                "Invalid table name format. Must start with a letter and contain only letters, numbers, and underscores"
            )

    def create_kv_table(self, table_name: str) -> None:
        """创建新的KV存储表"""
        try:
            self._validate_table_name(table_name)
            db_path = self._get_db_path(table_name)
            with self._lock:
                if os.path.exists(db_path):
                    raise StoreError(f"Table already exists: {table_name}")
                Path(db_path).write_text("{}", encoding="utf-8")
                self._add_table_info(table_name, "kv")
            logger.info(f"Created KV table: {table_name}")
        except StoreError:
            raise
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to create KV table: {str(e)}")
            raise StoreError(f"创建 TinyDB 表失败（表：{table_name}）", cause=e) from e

    def create_kkv_table(self, table_name: str) -> None:
        """创建新的KKV存储表"""
        try:
            self._validate_table_name(table_name)
            db_path = self._get_db_path(table_name)
            with self._lock:
                if os.path.exists(db_path):
                    raise StoreError(f"Table already exists: {table_name}")
                Path(db_path).write_text("{}", encoding="utf-8")
                self._add_table_info(table_name, "kkv")
            logger.info(f"Created KKV table: {table_name}")
        except StoreError:
            raise
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to create KKV table: {str(e)}")
            raise StoreError(f"创建 TinyDB 表失败（表：{table_name}）", cause=e) from e

    def get_table(self, table_name: str) -> TinyDBKVTable | TinyDBKKVTable:
        """获取指定的存储表接口"""
        try:
            self._validate_table_name(table_name)
            with self._lock:
                table_type = self._get_table_type(table_name)
                db_path = self._get_db_path(table_name)

                if not os.path.exists(db_path):
                    raise StoreError(f"Table file not found: {table_name}")

                if table_type == "kv":
                    return TinyDBKVTable(table_name, db_path)
                if table_type == "kkv":
                    return TinyDBKKVTable(table_name, db_path)
                raise StoreError(f"Invalid table type: {table_type}")
        except StoreError:
            raise
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to get table: {str(e)}")
            raise StoreError(f"获取 TinyDB 表失败（表：{table_name}）", cause=e) from e

    def list_tables(self) -> dict[str, str]:
        """获取所有表名列表"""
        try:
            with self._lock:
                table = self.db.table(self.TABLE_INFO_TABLE)
                return {
                    doc["name"]: doc["type"] for doc in table.all() if "name" in doc
                }
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to list tables: {str(e)}")
            raise StoreError("列出 TinyDB 表失败", cause=e) from e

    def drop_table(self, table_name: str) -> None:
        """删除指定的存储表"""
        try:
            self._validate_table_name(table_name)
            db_path = self._get_db_path(table_name)
            with self._lock:
                if not os.path.exists(db_path):
                    raise StoreError(f"Table not found: {table_name}")
                self._close_db(db_path)
                os.remove(db_path)
                self._remove_table_info(table_name)
            logger.info(f"Dropped table: {table_name}")
        except StoreError:
            raise
        except _TINYDB_OPERATION_ERRORS as e:
            logger.error(f"Failed to drop table: {str(e)}")
            raise StoreError(f"删除 TinyDB 表失败（表：{table_name}）", cause=e) from e
