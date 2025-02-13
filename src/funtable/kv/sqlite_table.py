"""
SQLite存储实现模块

基于SQLite实现KV和KKV存储接口。
使用SQLite的表结构存储键值对数据，值以JSON格式序列化存储。
"""

import json
import re
import sqlite3
import threading
from typing import Dict, Optional, Union

from funutil import get_logger

from .interface import (
    BaseDB,
    BaseKKVTable,
    BaseKVTable,
    StoreError,
)

logger = get_logger("funtable")


class SQLiteTableBase:
    """SQLite表基类"""

    def __init__(self, db_path: str):
        """初始化SQLite连接"""
        self.db_path = db_path
        self._local = threading.local()
        self._init_thread_local()

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

    def _init_thread_local(self):
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
            except Exception as e:
                logger.error(f"Failed to connect to SQLite database: {str(e)}")
                raise StoreError(f"Database connection failed: {str(e)}")
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
        except Exception as e:
            logger.error(f"SQLite error executing {sql}: {str(e)}")
            if not self._local.in_transaction:
                self.connection.rollback()
            raise StoreError(f"Database operation failed: {str(e)}")

    def close(self):
        """关闭数据库连接"""
        self._init_thread_local()
        if self._local.connection is not None:
            try:
                if self._local.in_transaction:
                    self._local.connection.rollback()
                self._local.connection.close()
                self._local.connection = None
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")
                raise StoreError(f"Failed to close database: {str(e)}")

    def __del__(self):
        """析构函数"""
        self.close()

    def begin_transaction(self) -> None:
        """开始事务"""
        self._init_thread_local()
        if self._local.in_transaction:
            raise StoreError("Already in transaction")
        try:
            self.connection.execute("BEGIN")
            self._local.in_transaction = True
        except Exception as e:
            logger.error(f"Error starting transaction: {str(e)}")
            raise StoreError(f"Failed to start transaction: {str(e)}")

    def commit(self) -> None:
        """提交事务"""
        self._init_thread_local()
        if not self._local.in_transaction:
            raise StoreError("Not in transaction")
        try:
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error committing transaction: {str(e)}")
            raise StoreError(f"Failed to commit transaction: {str(e)}")
        finally:
            self._local.in_transaction = False

    def rollback(self) -> None:
        """回滚事务"""
        self._init_thread_local()
        if not self._local.in_transaction:
            raise StoreError("Not in transaction")
        try:
            self.connection.rollback()
        except Exception as e:
            logger.error(f"Error rolling back transaction: {str(e)}")
            raise StoreError(f"Failed to rollback transaction: {str(e)}")
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
        self.table_name = table_name
        self._init_table()

    def _init_table(self):
        """初始化表结构"""
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

    def _validate_key(self, key: str):
        """验证键的类型"""
        if not isinstance(key, str):
            raise StoreError(f"Key must be string, got {type(key)}")

    def _validate_value(self, value: Dict):
        """验证值的类型"""
        if not isinstance(value, dict):
            raise StoreError(f"Value must be dict, got {type(value)}")

    def set(self, key: str, value: Dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(key)
            self._validate_value(value)
            self._execute(
                f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
        except Exception as e:
            logger.error(f"Error setting KV pair: {str(e)}")
            raise StoreError(f"Failed to set value: {str(e)}")

    def get(self, key: str) -> Optional[Dict]:
        """获取键的值"""
        try:
            self._validate_key(key)
            cursor = self._execute(
                f"SELECT value FROM {self.table_name} WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            return json.loads(row[0]) if row else None
        except Exception as e:
            logger.error(f"Error getting value for key {key}: {str(e)}")
            raise StoreError(f"Failed to get value: {str(e)}")

    def delete(self, key: str) -> bool:
        """删除键值对"""
        try:
            self._validate_key(key)
            cursor = self._execute(
                f"DELETE FROM {self.table_name} WHERE key = ?",
                (key,),
            )
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deleting key {key}: {str(e)}")
            raise StoreError(f"Failed to delete value: {str(e)}")

    def list_keys(self) -> list[str]:
        """列出所有键"""
        try:
            cursor = self._execute(f"SELECT key FROM {self.table_name}")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error listing keys: {str(e)}")
            raise StoreError(f"Failed to list keys: {str(e)}")

    def list_all(self) -> Dict[str, Dict]:
        """列出所有键值对"""
        try:
            cursor = self._execute(f"SELECT key, value FROM {self.table_name}")
            return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error listing all KV pairs: {str(e)}")
            raise StoreError(f"Failed to list all: {str(e)}")

    def batch_set(self, items: Dict[str, Dict]) -> None:
        """批量设置键值对"""
        try:
            values = [(k, json.dumps(v)) for k, v in items.items()]
            cursor = self.connection.cursor()
            cursor.executemany(
                f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                values,
            )
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError(f"Failed to perform batch set: {str(e)}")

    def batch_delete(self, keys: list[str]) -> int:
        """批量删除键值对"""
        try:
            cursor = self.connection.cursor()
            cursor.executemany(
                f"DELETE FROM {self.table_name} WHERE key = ?",
                [(k,) for k in keys],
            )
            deleted = cursor.rowcount
            self.connection.commit()
            return deleted
        except Exception as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError(f"Failed to perform batch delete: {str(e)}")


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
        self.table_name = table_name
        self._init_table()

    def _init_table(self):
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

    def _validate_key(self, key: str):
        """验证键的类型"""
        if not isinstance(key, str):
            raise StoreError(f"Key must be string, got {type(key)}")

    def _validate_value(self, value: Dict):
        """验证值的类型"""
        if not isinstance(value, dict):
            raise StoreError(f"Value must be dict, got {type(value)}")

    def set(self, pkey: str, skey: str, value: Dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(pkey)
            self._validate_key(skey)
            self._validate_value(value)
            self._execute(
                f"INSERT OR REPLACE INTO {self.table_name} (key1, key2, value) VALUES (?, ?, ?)",
                (pkey, skey, json.dumps(value)),
            )
        except Exception as e:
            logger.error(f"Error setting KKV pair: {str(e)}")
            raise StoreError(f"Failed to set value: {str(e)}")

    def get(self, pkey: str, skey: str) -> Optional[Dict]:
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
        except Exception as e:
            logger.error(f"Error getting value for key {pkey}, {skey}: {str(e)}")
            raise StoreError(f"Failed to get value: {str(e)}")

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
        except Exception as e:
            logger.error(f"Error deleting key {pkey}, {skey}: {str(e)}")
            raise StoreError(f"Failed to delete value: {str(e)}")

    def list_pkeys(self) -> list[str]:
        """列出所有主键"""
        try:
            cursor = self._execute(f"SELECT DISTINCT key1 FROM {self.table_name}")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error listing pkeys: {str(e)}")
            raise StoreError(f"Failed to list pkeys: {str(e)}")

    def list_skeys(self, pkey: str) -> list[str]:
        """列出所有次键"""
        try:
            cursor = self._execute(
                f"SELECT key2 FROM {self.table_name} WHERE key1 = ?",
                (pkey,),
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error listing skeys for pkey {pkey}: {str(e)}")
            raise StoreError(f"Failed to list skeys: {str(e)}")

    def list_all(self) -> Dict[str, Dict[str, Dict]]:
        """列出所有键值对"""
        try:
            cursor = self._execute(f"SELECT key1, key2, value FROM {self.table_name}")
            result: Dict[str, Dict[str, Dict]] = {}
            for row in cursor.fetchall():
                key1, key2, value_json = row
                if key1 not in result:
                    result[key1] = {}
                result[key1][key2] = json.loads(value_json)
            return result
        except Exception as e:
            logger.error(f"Error listing all KKV pairs: {str(e)}")
            raise StoreError(f"Failed to list all: {str(e)}")

    def batch_set(self, items: Dict[str, Dict[str, Dict]]) -> None:
        """批量设置键值对"""
        try:
            values = [
                (pk, sk, json.dumps(v))
                for pk, sdict in items.items()
                for sk, v in sdict.items()
            ]
            cursor = self.connection.cursor()
            cursor.executemany(
                f"INSERT OR REPLACE INTO {self.table_name} (key1, key2, value) VALUES (?, ?, ?)",
                values,
            )
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError(f"Failed to perform batch set: {str(e)}")

    def batch_delete(self, items: list[tuple[str, str]]) -> int:
        """批量删除键值对"""
        try:
            cursor = self.connection.cursor()
            cursor.executemany(
                f"DELETE FROM {self.table_name} WHERE key1 = ? AND key2 = ?",
                items,
            )
            deleted = cursor.rowcount
            self.connection.commit()
            return deleted
        except Exception as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError(f"Failed to perform batch delete: {str(e)}")


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
        return result[0]

    def _ensure_table_exists(self, table_name: str) -> None:
        """确保表存在"""
        cursor = self._execute(
            f"SELECT name FROM {self.TABLE_INFO_TABLE} WHERE name = ?",
            (table_name,),
        )
        if cursor.fetchone() is None:
            raise StoreError(f"Table '{table_name}' does not exist")

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
        self._validate_table_name(table_name)
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS {} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """.format(table_name)
        )
        self._add_table_info(table_name, "kv")
        logger.success(f"created KV table: {table_name} success")

    def create_kkv_table(self, table_name: str) -> None:
        self._validate_table_name(table_name)
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS {} (
                key1 TEXT,
                key2 TEXT,
                value TEXT NOT NULL,
                PRIMARY KEY (key1, key2)
            )
        """.format(table_name)
        )
        self._add_table_info(table_name, "kkv")
        logger.success(f"created KKV table: {table_name} success")

    def get_table(self, table_name: str) -> Union[BaseKVTable, BaseKKVTable]:
        self._ensure_table_exists(table_name)
        table_type = self._get_table_type(table_name)
        if table_type == "kv":
            return SQLiteKVTable(self.db_path, table_name)
        else:
            return SQLiteKKVTable(self.db_path, table_name)

    def list_tables(self) -> Dict[str, str]:
        cursor = self._execute(
            f"""
            SELECT name, type FROM {self.TABLE_INFO_TABLE}
            ORDER BY name
            """
        )
        return {row[0]: row[1] for row in cursor.fetchall()}

    def drop_table(self, table_name: str) -> None:
        self._ensure_table_exists(table_name)
        self._execute(f"DROP TABLE IF EXISTS {table_name}")
        self._remove_table_info(table_name)
