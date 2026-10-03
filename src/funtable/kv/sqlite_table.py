"""
SQLite存储实现模块

基于SQLite实现KV和KKV存储接口。
使用SQLite的表结构存储键值对数据，值以JSON格式序列化存储。
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading

from farlog import getLogger

from .interface import (
    BaseDB,
    BaseKKVTable,
    BaseKVTable,
    StoreError,
)

logger = getLogger("funtable")

_SQLITE_OPERATION_ERRORS = (sqlite3.Error, OSError, TypeError, ValueError)


class _SQLiteLocal(threading.local):
    """每个线程独立保存连接和事务状态。"""

    def __init__(self) -> None:
        self.in_transaction = False
        self.connection: sqlite3.Connection | None = None


class SQLiteTableBase:
    """SQLite表基类"""

    def __init__(self, db_path: str):
        """初始化SQLite连接"""
        self.db_path = db_path
        self._local = _SQLiteLocal()
        self._init_thread_local()

    def _validate_table_name(self, table_name: str) -> None:
        """验证表名是否有效"""
        if not table_name:
            raise StoreError("Table name cannot be empty")
        if len(table_name) > 128:
            raise StoreError("Table name too long (max 128 characters)")
        if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", table_name):
            raise StoreError(
                "Invalid table name format. Must start with a letter and contain only letters, numbers, and underscores"
            )

    def _validate_key(self, key: str) -> None:
        if not isinstance(key, str):
            raise StoreError(f"Key must be string, got {type(key)}")

    def _validate_value(self, value: dict) -> None:
        if not isinstance(value, dict):
            raise StoreError(f"Value must be dict, got {type(value)}")

    def _init_thread_local(self) -> None:
        """初始化线程本地存储"""
        if not hasattr(self._local, "in_transaction"):
            self._local.in_transaction = False
        if not hasattr(self._local, "connection"):
            self._local.connection = None

    @property
    def connection(self) -> sqlite3.Connection:
        """获取数据库连接，每个线程一个独立连接"""
        self._init_thread_local()
        if self._local.connection is None:
            try:
                self._local.connection = sqlite3.connect(self.db_path)
                self._local.connection.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                logger.error(f"Failed to connect to SQLite database: {str(e)}")
                raise StoreError(
                    f"连接 SQLite 数据库失败（路径：{self.db_path}）", cause=e
                ) from e
        return self._local.connection

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """执行SQL语句"""
        self._init_thread_local()
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            if not self._local.in_transaction:
                self.connection.commit()
            return cursor
        except sqlite3.Error as e:
            logger.error(f"SQLite error executing {sql}: {str(e)}")
            if not self._local.in_transaction:
                try:
                    self.connection.rollback()
                except sqlite3.Error as rollback_error:
                    logger.error(f"SQLite 回滚失败：{rollback_error}")
            raise StoreError(f"执行 SQLite 语句失败：{sql}", cause=e) from e

    def _executemany(self, sql: str, params: list[tuple]) -> sqlite3.Cursor:
        """批量执行SQL语句"""
        self._init_thread_local()
        try:
            cursor = self.connection.cursor()
            cursor.executemany(sql, params)
            if not self._local.in_transaction:
                self.connection.commit()
            return cursor
        except sqlite3.Error as e:
            if not self._local.in_transaction:
                try:
                    self.connection.rollback()
                except sqlite3.Error as rollback_error:
                    logger.error(f"SQLite 回滚失败：{rollback_error}")
            raise StoreError(f"批量执行 SQLite 语句失败：{sql}", cause=e) from e

    def close(self) -> None:
        """关闭数据库连接"""
        self._init_thread_local()
        if self._local.connection is not None:
            try:
                if self._local.in_transaction:
                    self._local.connection.rollback()
                self._local.connection.close()
                self._local.connection = None
            except sqlite3.Error as e:
                logger.error(f"Error closing database connection: {str(e)}")
                raise StoreError(
                    f"关闭 SQLite 数据库失败（路径：{self.db_path}）", cause=e
                ) from e

    def __del__(self) -> None:
        """析构函数"""
        try:
            self.close()
        except StoreError as e:
            logger.warning(f"析构时关闭 SQLite 连接失败，已忽略：{e}")

    def begin_transaction(self) -> None:
        """开始事务"""
        self._init_thread_local()
        if self._local.in_transaction:
            raise StoreError("Already in transaction")
        try:
            self.connection.execute("BEGIN")
            self._local.in_transaction = True
        except sqlite3.Error as e:
            logger.error(f"Error starting transaction: {str(e)}")
            raise StoreError("启动 SQLite 事务失败", cause=e) from e

    def commit(self) -> None:
        """提交事务"""
        self._init_thread_local()
        if not self._local.in_transaction:
            raise StoreError("Not in transaction")
        try:
            self.connection.commit()
        except sqlite3.Error as e:
            logger.error(f"Error committing transaction: {str(e)}")
            raise StoreError("提交 SQLite 事务失败", cause=e) from e
        finally:
            self._local.in_transaction = False

    def rollback(self) -> None:
        """回滚事务"""
        self._init_thread_local()
        if not self._local.in_transaction:
            raise StoreError("Not in transaction")
        try:
            self.connection.rollback()
        except sqlite3.Error as e:
            logger.error(f"Error rolling back transaction: {str(e)}")
            raise StoreError("回滚 SQLite 事务失败", cause=e) from e
        finally:
            self._local.in_transaction = False


class SQLiteKVTable(SQLiteTableBase, BaseKVTable):
    """SQLite的KV存储实现类

    表结构:
    - key: TEXT PRIMARY KEY  # 键
    - value: TEXT NOT NULL   # JSON序列化的字典值
    """

    def __init__(self, db_path: str, table_name: str):
        super().__init__(db_path)
        self._validate_table_name(table_name)
        self.table_name = table_name
        self._init_table()

    def _init_table(self) -> None:
        """初始化表结构"""
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

    def set(self, key: str, value: dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(key)
            self._validate_value(value)
            self._execute(
                f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error setting KV pair: {str(e)}")
            raise StoreError("设置 SQLite KV 数据失败", cause=e) from e

    def get(self, key: str) -> dict | None:
        """获取键的值"""
        try:
            self._validate_key(key)
            cursor = self._execute(
                f"SELECT value FROM {self.table_name} WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            return json.loads(row[0]) if row else None
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error getting value for key {key}: {str(e)}")
            raise StoreError(f"读取 SQLite KV 数据失败（键：{key}）", cause=e) from e

    def delete(self, key: str) -> bool:
        """删除键值对"""
        try:
            self._validate_key(key)
            cursor = self._execute(
                f"DELETE FROM {self.table_name} WHERE key = ?",
                (key,),
            )
            return cursor.rowcount > 0
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error deleting key {key}: {str(e)}")
            raise StoreError(f"删除 SQLite KV 数据失败（键：{key}）", cause=e) from e

    def list_keys(self) -> list[str]:
        """列出所有键"""
        try:
            cursor = self._execute(f"SELECT key FROM {self.table_name}")
            return [row[0] for row in cursor.fetchall()]
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error listing keys: {str(e)}")
            raise StoreError("列出 SQLite KV 键失败", cause=e) from e

    def list_all(self) -> dict[str, dict]:
        """列出所有键值对"""
        try:
            cursor = self._execute(f"SELECT key, value FROM {self.table_name}")
            return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error listing all KV pairs: {str(e)}")
            raise StoreError("列出 SQLite KV 数据失败", cause=e) from e

    def batch_set(self, items: dict[str, dict]) -> None:
        """批量设置键值对"""
        try:
            for key, value in items.items():
                self._validate_key(key)
                self._validate_value(value)
            values = [(k, json.dumps(v)) for k, v in items.items()]
            self._executemany(
                f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                values,
            )
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError("批量设置 SQLite KV 数据失败", cause=e) from e

    def batch_delete(self, keys: list[str]) -> int:
        """批量删除键值对"""
        try:
            for key in keys:
                self._validate_key(key)
            cursor = self._executemany(
                f"DELETE FROM {self.table_name} WHERE key = ?",
                [(k,) for k in keys],
            )
            return cursor.rowcount
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError("批量删除 SQLite KV 数据失败", cause=e) from e


class SQLiteKKVTable(SQLiteTableBase, BaseKKVTable):
    """SQLite的KKV存储实现类

    表结构:
    - key1: TEXT            # 主键
    - key2: TEXT            # 次键
    - value: TEXT NOT NULL  # JSON序列化的字典值
    - PRIMARY KEY (key1, key2)
    """

    def __init__(self, db_path: str, table_name: str):
        super().__init__(db_path)
        self._validate_table_name(table_name)
        self.table_name = table_name
        self._init_table()

    def _init_table(self) -> None:
        """初始化表结构"""
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                key1 TEXT,
                key2 TEXT,
                value TEXT NOT NULL,
                PRIMARY KEY (key1, key2)
            )
            """
        )

    def set(self, pkey: str, skey: str, value: dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(pkey)
            self._validate_key(skey)
            self._validate_value(value)
            self._execute(
                f"INSERT OR REPLACE INTO {self.table_name} (key1, key2, value) VALUES (?, ?, ?)",
                (pkey, skey, json.dumps(value)),
            )
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error setting KKV pair: {str(e)}")
            raise StoreError("设置 SQLite KKV 数据失败", cause=e) from e

    def get(self, pkey: str, skey: str) -> dict | None:
        """获取键的值"""
        try:
            self._validate_key(pkey)
            self._validate_key(skey)
            cursor = self._execute(
                f"SELECT value FROM {self.table_name} WHERE key1 = ? AND key2 = ?",
                (pkey, skey),
            )
            row = cursor.fetchone()
            return json.loads(row[0]) if row else None
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error getting value for key {pkey}, {skey}: {str(e)}")
            raise StoreError(
                f"读取 SQLite KKV 数据失败（主键：{pkey}，次键：{skey}）", cause=e
            ) from e

    def delete(self, pkey: str, skey: str) -> bool:
        """删除键值对"""
        try:
            self._validate_key(pkey)
            self._validate_key(skey)
            cursor = self._execute(
                f"DELETE FROM {self.table_name} WHERE key1 = ? AND key2 = ?",
                (pkey, skey),
            )
            return cursor.rowcount > 0
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error deleting key {pkey}, {skey}: {str(e)}")
            raise StoreError(
                f"删除 SQLite KKV 数据失败（主键：{pkey}，次键：{skey}）", cause=e
            ) from e

    def list_pkeys(self) -> list[str]:
        """列出所有主键"""
        try:
            cursor = self._execute(f"SELECT DISTINCT key1 FROM {self.table_name}")
            return [row[0] for row in cursor.fetchall()]
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error listing pkeys: {str(e)}")
            raise StoreError("列出 SQLite KKV 主键失败", cause=e) from e

    def list_skeys(self, pkey: str) -> list[str]:
        """列出所有次键"""
        try:
            cursor = self._execute(
                f"SELECT key2 FROM {self.table_name} WHERE key1 = ?",
                (pkey,),
            )
            return [row[0] for row in cursor.fetchall()]
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error listing skeys for pkey {pkey}: {str(e)}")
            raise StoreError(f"列出 SQLite KKV 次键失败（主键：{pkey}）", cause=e) from e

    def list_all(self) -> dict[str, dict[str, dict]]:
        """列出所有键值对"""
        try:
            cursor = self._execute(f"SELECT key1, key2, value FROM {self.table_name}")
            result: dict[str, dict[str, dict]] = {}
            for row in cursor.fetchall():
                key1, key2, value_json = row
                if key1 not in result:
                    result[key1] = {}
                result[key1][key2] = json.loads(value_json)
            return result
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error listing all KKV pairs: {str(e)}")
            raise StoreError("列出 SQLite KKV 数据失败", cause=e) from e

    def batch_set(self, items: dict[str, dict[str, dict]]) -> None:
        """批量设置键值对"""
        try:
            for pkey, skeys in items.items():
                self._validate_key(pkey)
                for skey, value in skeys.items():
                    self._validate_key(skey)
                    self._validate_value(value)
            values = [
                (pk, sk, json.dumps(v))
                for pk, sdict in items.items()
                for sk, v in sdict.items()
            ]
            self._executemany(
                f"INSERT OR REPLACE INTO {self.table_name} (key1, key2, value) VALUES (?, ?, ?)",
                values,
            )
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError("批量设置 SQLite KKV 数据失败", cause=e) from e

    def batch_delete(self, items: list[tuple[str, str]]) -> int:
        """批量删除键值对"""
        try:
            for pkey, skey in items:
                self._validate_key(pkey)
                self._validate_key(skey)
            cursor = self._executemany(
                f"DELETE FROM {self.table_name} WHERE key1 = ? AND key2 = ?",
                items,
            )
            return cursor.rowcount
        except _SQLITE_OPERATION_ERRORS as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError("批量删除 SQLite KKV 数据失败", cause=e) from e


class SQLiteStore(SQLiteTableBase, BaseDB):
    """SQLite数据库维度存储实现"""

    TABLE_INFO_TABLE = "_table_info"  # 存储表信息的表名

    def __init__(self, db_path: str = "sqlite_store.db"):
        """初始化SQLite存储

        Args:
            db_path: SQLite数据库文件路径，默认为sqlite_store.db
        """
        SQLiteTableBase.__init__(self, db_path)
        self._init_table_info_table()

    def _init_table_info_table(self) -> None:
        """初始化存储表信息表"""
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_INFO_TABLE} (
                name TEXT PRIMARY KEY,
                type TEXT NOT NULL CHECK(type IN ('kv', 'kkv')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _add_table_info(self, table_name: str, table_type: str) -> None:
        """添加或更新存储表信息

        如果表已存在，则更新其信息；如果不存在，则添加新记录。

        Args:
            table_name: 存储表名
            table_type: 存储表类型 ("kv" 或 "kkv")
        """
        self._execute(
            f"""
            INSERT OR REPLACE INTO {self.TABLE_INFO_TABLE} (name, type, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (table_name, table_type),
        )

    def _remove_table_info(self, table_name: str) -> None:
        """删除存储表信息"""
        self._execute(
            f"DELETE FROM {self.TABLE_INFO_TABLE} WHERE name = ?",
            (table_name,),
        )

    def _get_table_type(self, table_name: str) -> str:
        """获取存储表类型"""
        cursor = self._execute(
            f"SELECT type FROM {self.TABLE_INFO_TABLE} WHERE name = ?",
            (table_name,),
        )
        result = cursor.fetchone()
        if result is None:
            raise StoreError(f"Table '{table_name}' does not exist")
        return str(result[0])

    def create_kv_table(self, table_name: str) -> None:
        """创建名为 ``table_name`` 的单键表。

        Args:
            table_name: 表名，只能以字母开头、仅含字母数字下划线。

        表已存在时直接复用（``CREATE TABLE IF NOT EXISTS``），不会报错。
        """
        self._validate_table_name(table_name)
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """
        )
        self._add_table_info(table_name, "kv")
        logger.info(f"created KV table: {table_name} success")

    def create_kkv_table(self, table_name: str) -> None:
        """创建名为 ``table_name`` 的双键表。

        Args:
            table_name: 表名，只能以字母开头、仅含字母数字下划线。

        表已存在时直接复用（``CREATE TABLE IF NOT EXISTS``），不会报错。
        """
        self._validate_table_name(table_name)
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                key1 TEXT,
                key2 TEXT,
                value TEXT NOT NULL,
                PRIMARY KEY (key1, key2)
            )
        """
        )
        self._add_table_info(table_name, "kkv")
        logger.info(f"created KKV table: {table_name} success")

    def get_table(self, table_name: str) -> BaseKVTable | BaseKKVTable:
        """返回指定表的操作对象。

        Args:
            table_name: 表名。

        Returns:
            根据表登记的类型返回 ``SQLiteKVTable`` 或 ``SQLiteKKVTable``。

        Raises:
            StoreError: 表不存在时抛出。
        """
        self._validate_table_name(table_name)
        table_type = self._get_table_type(table_name)
        if table_type == "kv":
            return SQLiteKVTable(self.db_path, table_name)
        return SQLiteKKVTable(self.db_path, table_name)

    def list_tables(self) -> dict[str, str]:
        """返回 ``表名 -> 表类型`` 映射，按表名排序。"""
        cursor = self._execute(
            f"""
            SELECT name, type FROM {self.TABLE_INFO_TABLE}
            ORDER BY name
            """
        )
        return {row[0]: row[1] for row in cursor.fetchall()}

    def drop_table(self, table_name: str) -> None:
        """删除指定表及其登记信息。

        Args:
            table_name: 表名。

        Raises:
            StoreError: 表不存在时抛出。
        """
        self._validate_table_name(table_name)
        self._get_table_type(table_name)
        self._execute(f"DROP TABLE IF EXISTS {table_name}")
        self._remove_table_info(table_name)
