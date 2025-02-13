"""
SQLite存储实现模块

基于SQLite实现KV和KKV存储接口。
使用SQLite的表结构存储键值对数据，值以JSON格式序列化存储。
"""

import json
import re
import sqlite3
import time
from typing import Dict, Optional, Union

from funutil import getLogger

from .interface import (
    BaseDB,
    BaseKKVTable,
    BaseKVTable,
    StoreError,
)

logger = getLogger("funkv")


class SQLiteTableBase:
    """SQLite数据库连接管理基类

    实现数据库连接的单例模式管理，确保同一数据库文件只创建一个连接实例。
    提供SQL执行的基础方法。
    """

    def __init__(self, db_path: str):
        """初始化SQLite数据库连接

        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
        self._connection = None

    @property
    def connection(self) -> sqlite3.Connection:
        """获取数据库连接（单例模式）"""
        if self._connection is None:
            self._connection = self._create_connection()
        return self._connection

    def _create_connection(self):
        # 添加重试逻辑
        retries = 3
        while retries > 0:
            try:
                conn = sqlite3.connect(
                    self.db_path, check_same_thread=False, timeout=20
                )
                conn.row_factory = sqlite3.Row
                return conn
            except sqlite3.Error:
                retries -= 1
                if retries == 0:
                    raise
                time.sleep(1)

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """执行SQL语句"""
        logger.debug(f"executing SQL: {sql} with params: {params}")
        cursor = self.connection.cursor()
        cursor.execute(sql, params)
        self.connection.commit()
        return cursor

    def __del__(self):
        """析构函数，确保连接被正确关闭"""
        if self._connection is not None:
            logger.debug(f"closing SQLite connection for {self.db_path}")
            self._connection.close()
            self._connection = None


class SQLiteKVTable(SQLiteTableBase, BaseKVTable):
    """SQLite的KV存储实现类

    表结构:
    - key: TEXT PRIMARY KEY  # 键
    - value: TEXT NOT NULL   # JSON序列化的字典值
    """

    def __init__(self, db_path: str, table_name: str):
        super().__init__(db_path)
        self.table_name = table_name

    def _validate_key(self, key: str) -> None:
        """验证键的类型"""
        if not isinstance(key, str):
            raise StoreError("Key must be string type")

    def _validate_value(self, value: Dict) -> None:
        """验证值的类型"""
        if not isinstance(value, dict):
            raise StoreError("Value must be dict type")

    def set(self, key: str, value: Dict) -> None:
        try:
            self._validate_key(key)
            self._validate_value(value)
            value_json = json.dumps(value)
            self._execute(
                f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                (key, value_json),
            )
        except StoreError as e:
            logger.error(f"failed to set KV pair: {str(e)}")
            raise

    def get(self, key: str) -> Optional[Dict]:
        cursor = self._execute(
            f"SELECT value FROM {self.table_name} WHERE key = ?", (key,)
        )
        result = cursor.fetchone()
        return json.loads(result[0]) if result else None

    def delete(self, key: str) -> bool:
        logger.debug(f"deleting key={key}")
        cursor = self._execute(f"DELETE FROM {self.table_name} WHERE key = ?", (key,))
        return cursor.rowcount > 0

    def list_keys(self) -> list[str]:
        cursor = self._execute(f"SELECT key FROM {self.table_name}")
        return [row[0] for row in cursor.fetchall()]

    def list_all(self) -> Dict[str, Dict]:
        cursor = self._execute(f"SELECT key, value FROM {self.table_name}")
        return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}

    def batch_set(self, items: Dict[str, Dict]) -> None:
        """批量设置键值对"""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.executemany(
                f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                [(k, json.dumps(v)) for k, v in items.items()],
            )

    def batch_delete(self, keys: list[str]) -> int:
        """批量删除键值对"""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(
                f"DELETE FROM {self.table_name} WHERE key IN ({','.join('?' * len(keys))})",
                keys,
            )
            return cursor.rowcount

    def begin_transaction(self) -> None:
        """开始事务"""
        self._execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        """提交事务"""
        self._execute("COMMIT")

    def rollback(self) -> None:
        """回滚事务"""
        self._execute("ROLLBACK")


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

    def _validate_key(self, key: str) -> None:
        """验证键的类型"""
        if not isinstance(key, str):
            raise StoreError("Key must be string type")

    def _validate_value(self, value: Dict) -> None:
        """验证值的类型"""
        if not isinstance(value, dict):
            raise StoreError("Value must be dict type")

    def set(self, pkey: str, skey: str, value: Dict) -> None:
        try:
            self._validate_key(pkey)
            self._validate_key(skey)
            self._validate_value(value)
            logger.debug(f"setting KKV pair: pkey={pkey}, skey={skey}")
            value_json = json.dumps(value)
            self._execute(
                f"INSERT OR REPLACE INTO {self.table_name} (key1, key2, value) VALUES (?, ?, ?)",
                (pkey, skey, value_json),
            )
        except StoreError as e:
            logger.error(f"failed to set KKV pair: {str(e)}")
            raise

    def get(self, pkey: str, skey: str) -> Optional[Dict]:
        logger.debug(f"getting value for pkey={pkey}, skey={skey}")
        cursor = self._execute(
            f"SELECT value FROM {self.table_name} WHERE key1 = ? AND key2 = ?",
            (pkey, skey),
        )
        result = cursor.fetchone()
        return json.loads(result[0]) if result else None

    def delete(self, pkey: str, skey: str) -> bool:
        logger.debug(f"deleting pkey={pkey}, skey={skey}")
        cursor = self._execute(
            f"DELETE FROM {self.table_name} WHERE key1 = ? AND key2 = ?",
            (pkey, skey),
        )
        return cursor.rowcount > 0

    def list_pkeys(self) -> list[str]:
        cursor = self._execute(f"SELECT DISTINCT key1 FROM {self.table_name}")
        return [row[0] for row in cursor.fetchall()]

    def list_skeys(self, pkey: str) -> list[str]:
        cursor = self._execute(
            f"SELECT key2 FROM {self.table_name} WHERE key1 = ?", (pkey,)
        )
        return [row[0] for row in cursor.fetchall()]

    def list_all(self) -> Dict[str, Dict[str, Dict]]:
        cursor = self._execute(f"SELECT key1, key2, value FROM {self.table_name}")
        result: Dict[str, Dict[str, Dict]] = {}
        for row in cursor.fetchall():
            key1, key2, value_json = row
            if key1 not in result:
                result[key1] = {}
            result[key1][key2] = json.loads(value_json)
        return result

    def begin_transaction(self) -> None:
        """开始事务"""
        self._execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        """提交事务"""
        self._execute("COMMIT")

    def rollback(self) -> None:
        """回滚事务"""
        self._execute("ROLLBACK")

    def batch_set(self, items: Dict[str, Dict[str, Dict]]) -> None:
        """批量设置键值对"""
        with self.connection:
            cursor = self.connection.cursor()
            values = [
                (pk, sk, json.dumps(v))
                for pk, sdict in items.items()
                for sk, v in sdict.items()
            ]
            cursor.executemany(
                f"INSERT OR REPLACE INTO {self.table_name} (key1, key2, value) VALUES (?, ?, ?)",
                values,
            )

    def batch_delete(self, items: list[tuple[str, str]]) -> int:
        """批量删除键值对"""
        with self.connection:
            cursor = self.connection.cursor()
            cursor.executemany(
                f"DELETE FROM {self.table_name} WHERE key1 = ? AND key2 = ?",
                items,
            )
            return cursor.rowcount


class SQLiteStore(SQLiteTableBase, BaseDB):
    """SQLite数据库维度存储实现"""

    def __init__(self, db_path: str = "sqlite_store.db"):
        super().__init__(db_path)
        self._table_name_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
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
        """验证表名是否合法"""
        if not isinstance(table_name, str):
            raise StoreError("Table name must be string type")
        if not self._table_name_pattern.match(table_name):
            raise StoreError(
                "Table name must start with a letter and contain only letters, numbers and underscores"
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
