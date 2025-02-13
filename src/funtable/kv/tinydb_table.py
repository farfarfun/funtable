"""
TinyDB存储实现模块

基于TinyDB实现KV和KKV存储接口。
TinyDB是一个轻量级的文档型数据库，数据以JSON格式存储。
"""

import os
import re
import threading
from datetime import datetime
from typing import Dict, Optional, Union

from funutil import get_logger
from tinydb import Query, TinyDB

from .interface import (
    BaseDB,
    BaseKKVTable,
    BaseKVTable,
    StoreError,
)

logger = get_logger("funtable")


class TinyDBTableBase:
    """TinyDB表基类"""

    # 数据库实例缓存
    _db_instances = {}
    _db_locks = {}

    def __init__(self, db_path: str):
        """初始化TinyDB连接"""
        self.db_path = db_path
        self._local = threading.local()
        self._init_thread_local()
        self._lock = threading.RLock()

        # 初始化数据库实例和锁
        if db_path not in TinyDBTableBase._db_locks:
            TinyDBTableBase._db_locks[db_path] = threading.RLock()

    def _init_thread_local(self):
        """初始化线程本地存储"""
        if not hasattr(self._local, "in_transaction"):
            self._local.in_transaction = False
        if not hasattr(self._local, "transaction_cache"):
            self._local.transaction_cache = []
        if not hasattr(self._local, "db"):
            self._local.db = None

    @property
    def db(self) -> TinyDB:
        """获取数据库连接，每个线程一个独立连接"""
        self._init_thread_local()
        if self._local.db is None:
            try:
                with TinyDBTableBase._db_locks[self.db_path]:
                    if self.db_path not in TinyDBTableBase._db_instances:
                        TinyDBTableBase._db_instances[self.db_path] = TinyDB(
                            self.db_path
                        )
                    self._local.db = TinyDBTableBase._db_instances[self.db_path]
            except Exception as e:
                logger.error(f"Failed to connect to TinyDB database: {str(e)}")
                raise StoreError(f"Database connection failed: {str(e)}")
        return self._local.db

    def close(self):
        """关闭数据库连接"""
        self._init_thread_local()
        if self._local.db is not None:
            try:
                if self._local.in_transaction:
                    self.rollback()
                with TinyDBTableBase._db_locks[self.db_path]:
                    if self.db_path in TinyDBTableBase._db_instances:
                        TinyDBTableBase._db_instances[self.db_path].close()
                        del TinyDBTableBase._db_instances[self.db_path]
                self._local.db = None
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")
                raise StoreError(f"Failed to close database: {str(e)}")

    def __del__(self):
        """析构函数"""
        self.close()

    def begin_transaction(self) -> None:
        """开始事务"""
        self._init_thread_local()
        with self._lock:
            if self._local.in_transaction:
                raise StoreError("Already in transaction")
            self._local.transaction_cache = []
            self._local.in_transaction = True

    def commit(self) -> None:
        """提交事务"""
        self._init_thread_local()
        with self._lock:
            if not self._local.in_transaction:
                raise StoreError("Not in transaction")
            try:
                for operation in self._local.transaction_cache:
                    operation()
                self._local.transaction_cache = []
            except Exception as e:
                logger.error(f"Error committing transaction: {str(e)}")
                self.rollback()
                raise StoreError(f"Failed to commit transaction: {str(e)}")
            finally:
                self._local.in_transaction = False

    def rollback(self) -> None:
        """回滚事务"""
        self._init_thread_local()
        with self._lock:
            if not self._local.in_transaction:
                raise StoreError("Not in transaction")
            try:
                self._local.transaction_cache = []
            except Exception as e:
                logger.error(f"Error rolling back transaction: {str(e)}")
                raise StoreError(f"Failed to rollback transaction: {str(e)}")
            finally:
                self._local.in_transaction = False

    def _add_to_transaction(self, operation):
        """添加操作到事务缓存"""
        self._init_thread_local()
        if self._local.in_transaction:
            self._local.transaction_cache.append(operation)
            return True
        return False

    def _validate_key(self, key: str) -> None:
        """验证键是否有效"""
        if not isinstance(key, str):
            raise StoreError("Key must be string type")
        if not key:
            raise StoreError("Key cannot be empty")
        if len(key) > 128:
            raise StoreError("Key too long (max 128 characters)")

    def _validate_value(self, value: Dict) -> None:
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
        self._init_thread_local()
        self._batch_size = 1000

    @property
    def table(self):
        """获取表对象"""
        return self.db.table(self.table_name)

    def set(self, key: str, value: Dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(key)
            self._validate_value(value)
            self._init_thread_local()

            if self._local.in_transaction:
                self._add_to_transaction(
                    lambda: self.table.upsert(
                        {"key": key, "value": value},
                        self.query.key == key,
                    )
                )
                return

            with self._lock:
                self.table.upsert(
                    {"key": key, "value": value},
                    self.query.key == key,
                )

        except Exception as e:
            logger.error(f"Error setting KV pair: {str(e)}")
            raise StoreError(f"Failed to set value: {str(e)}")

    def get(self, key: str) -> Optional[Dict]:
        """获取键值对"""
        try:
            self._validate_key(key)
            self._init_thread_local()

            with self._lock:
                result = self.table.get(self.query.key == key)
                return result["value"] if result else None

        except Exception as e:
            logger.error(f"Error getting value: {str(e)}")
            raise StoreError(f"Failed to get value: {str(e)}")

    def delete(self, key: str) -> bool:
        """删除键值对"""
        try:
            self._validate_key(key)
            self._init_thread_local()

            if self._local.in_transaction:
                self._add_to_transaction(
                    lambda: self.table.remove(self.query.key == key)
                )
                return True

            with self._lock:
                return len(self.table.remove(self.query.key == key)) > 0

        except Exception as e:
            logger.error(f"Error deleting KV pair: {str(e)}")
            raise StoreError(f"Failed to delete value: {str(e)}")

    def batch_set(self, items: Dict[str, Dict]) -> None:
        """批量设置键值对"""
        try:
            batch_data = []
            for key, value in items.items():
                self._validate_key(key)
                self._validate_value(value)
                batch_data.append({"key": key, "value": value})

                # Process in batches to avoid memory issues
                if len(batch_data) >= self._batch_size:
                    if self._local.in_transaction:
                        self._add_to_transaction(
                            lambda: self.table.insert_multiple(batch_data)
                        )
                    else:
                        with self._lock:
                            self.table.insert_multiple(batch_data)
                    batch_data = []

            if batch_data:
                if self._local.in_transaction:
                    self._add_to_transaction(
                        lambda: self.table.insert_multiple(batch_data)
                    )
                else:
                    with self._lock:
                        self.table.insert_multiple(batch_data)

        except Exception as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError(f"Failed to perform batch set: {str(e)}")

    def batch_delete(self, keys: list[str]) -> int:
        """批量删除键值对"""
        try:
            deleted = 0
            for i in range(0, len(keys), self._batch_size):
                batch = keys[i : i + self._batch_size]
                if self._local.in_transaction:
                    self._add_to_transaction(
                        lambda: self.table.remove(self.query.key.one_of(batch))
                    )
                else:
                    with self._lock:
                        result = self.table.remove(self.query.key.one_of(batch))
                        deleted += len(result)

            return deleted

        except Exception as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError(f"Failed to perform batch delete: {str(e)}")

    def list_keys(self) -> list[str]:
        """获取所有键列表

        Returns:
            包含所有键的列表
        """
        return [doc["key"] for doc in self.table.all()]

    def list_all(self) -> Dict[str, Dict]:
        """获取所有键值对数据

        Returns:
            包含所有键值对的字典，格式为 {key: value_dict}
        """
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
        self._init_thread_local()
        self._batch_size = 1000

    @property
    def table(self):
        """获取表对象"""
        return self.db.table(self.table_name)

    def set(self, key1: str, key2: str, value: Dict) -> None:
        """设置键值对"""
        try:
            self._validate_key(key1)
            self._validate_key(key2)
            self._validate_value(value)
            self._init_thread_local()

            if self._local.in_transaction:
                self._add_to_transaction(
                    lambda: self.table.upsert(
                        {"key1": key1, "key2": key2, "value": value},
                        (self.query.key1 == key1) & (self.query.key2 == key2),
                    )
                )
                return

            with self._lock:
                self.table.upsert(
                    {"key1": key1, "key2": key2, "value": value},
                    (self.query.key1 == key1) & (self.query.key2 == key2),
                )

        except Exception as e:
            logger.error(f"Error setting KKV pair: {str(e)}")
            raise StoreError(f"Failed to set value: {str(e)}")

    def get(self, key1: str, key2: str) -> Optional[Dict]:
        """获取键值对"""
        try:
            self._validate_key(key1)
            self._validate_key(key2)
            self._init_thread_local()

            with self._lock:
                result = self.table.get(
                    (self.query.key1 == key1) & (self.query.key2 == key2)
                )
                return result["value"] if result else None

        except Exception as e:
            logger.error(f"Error getting value: {str(e)}")
            raise StoreError(f"Failed to get value: {str(e)}")

    def delete(self, key1: str, key2: str) -> bool:
        """删除键值对"""
        try:
            self._validate_key(key1)
            self._validate_key(key2)
            self._init_thread_local()

            if self._local.in_transaction:
                self._add_to_transaction(
                    lambda: self.table.remove(
                        (self.query.key1 == key1) & (self.query.key2 == key2)
                    )
                )
                return True

            with self._lock:
                return (
                    len(
                        self.table.remove(
                            (self.query.key1 == key1) & (self.query.key2 == key2)
                        )
                    )
                    > 0
                )

        except Exception as e:
            logger.error(f"Error deleting KKV pair: {str(e)}")
            raise StoreError(f"Failed to delete value: {str(e)}")

    def batch_set(self, items: Dict[str, Dict[str, Dict]]) -> None:
        """批量设置键值对

        Args:
            items: 格式为 {pkey: {skey: value_dict}}
        """
        try:
            batch_data = []
            for pkey, skey_dict in items.items():
                for skey, value in skey_dict.items():
                    self._validate_key(pkey)
                    self._validate_key(skey)
                    self._validate_value(value)
                    batch_data.append({"key1": pkey, "key2": skey, "value": value})

                    # Process in batches to avoid memory issues
                    if len(batch_data) >= self._batch_size:
                        if self._local.in_transaction:
                            self._add_to_transaction(
                                lambda: self.table.insert_multiple(batch_data)
                            )
                        else:
                            with self._lock:
                                self.table.insert_multiple(batch_data)
                        batch_data = []

            if batch_data:
                if self._local.in_transaction:
                    self._add_to_transaction(
                        lambda: self.table.insert_multiple(batch_data)
                    )
                else:
                    with self._lock:
                        self.table.insert_multiple(batch_data)

        except Exception as e:
            logger.error(f"Error in batch set operation: {str(e)}")
            raise StoreError(f"Failed to perform batch set: {str(e)}")

    def batch_delete(self, items: list[tuple[str, str]]) -> None:
        """批量删除键值对

        Args:
            items: 要删除的键对列表 [(pkey, skey)]
        """
        try:
            deleted = 0
            for i in range(0, len(items), self._batch_size):
                batch = items[i : i + self._batch_size]
                if self._local.in_transaction:
                    self._add_to_transaction(
                        lambda: self.table.remove(
                            (self.query.key1.one_of([p[0] for p in batch]))
                            & (self.query.key2.one_of([p[1] for p in batch]))
                        )
                    )
                else:
                    with self._lock:
                        result = self.table.remove(
                            (self.query.key1.one_of([p[0] for p in batch]))
                            & (self.query.key2.one_of([p[1] for p in batch]))
                        )
                        deleted += len(result)

            return deleted

        except Exception as e:
            logger.error(f"Error in batch delete operation: {str(e)}")
            raise StoreError(f"Failed to perform batch delete: {str(e)}")

    def list_pkeys(self) -> list[str]:
        """获取所有第一级键列表"""
        return list(set(doc["key1"] for doc in self.table.all()))

    def list_skeys(self, pkey: str) -> list[str]:
        """获取指定第一级键下的所有第二级键列表"""
        self._validate_key(pkey)
        return [doc["key2"] for doc in self.table.search(self.query.key1 == pkey)]

    def list_all(self) -> Dict[str, Dict[str, Dict]]:
        """获取所有键值对数据"""
        result = {}
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
        except Exception as e:
            logger.error(f"Failed to initialize TinyDBStore: {str(e)}")
            raise StoreError(f"Store initialization failed: {str(e)}")

    def _init_table_info_table(self) -> None:
        """初始化存储表信息表"""
        try:
            table = self.db.table(self.TABLE_INFO_TABLE)
            if not table.all():
                logger.info("Initializing table info storage")
                table.insert({"created_at": datetime.now().isoformat()})
        except Exception as e:
            logger.error(f"Failed to initialize table info: {str(e)}")
            raise StoreError(f"Table info initialization failed: {str(e)}")

    def _add_table_info(self, table_name: str, table_type: str) -> None:
        """添加或更新存储表信息"""
        try:
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
        except Exception as e:
            logger.error(f"Failed to add/update table info: {str(e)}")
            raise StoreError(f"Failed to update table info: {str(e)}")

    def _remove_table_info(self, table_name: str) -> None:
        """删除存储表信息"""
        try:
            table = self.db.table(self.TABLE_INFO_TABLE)
            table.remove(Query().name == table_name)
            logger.info(f"Removed table info: {table_name}")
        except Exception as e:
            logger.error(f"Failed to remove table info: {str(e)}")
            raise StoreError(f"Failed to remove table info: {str(e)}")

    def _get_table_type(self, table_name: str) -> str:
        """获取存储表类型"""
        try:
            table = self.db.table(self.TABLE_INFO_TABLE)
            result = table.get(Query().name == table_name)
            if not result:
                raise StoreError(f"Table not found: {table_name}")
            return result["type"]
        except StoreError:
            raise
        except Exception as e:
            logger.error(f"Failed to get table type: {str(e)}")
            raise StoreError(f"Failed to get table type: {str(e)}")

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
            if os.path.exists(db_path):
                raise StoreError(f"Table already exists: {table_name}")

            # 创建表文件
            TinyDB(db_path).close()
            self._add_table_info(table_name, "kv")
            logger.info(f"Created KV table: {table_name}")
        except StoreError:
            raise
        except Exception as e:
            logger.error(f"Failed to create KV table: {str(e)}")
            raise StoreError(f"Failed to create table: {str(e)}")

    def create_kkv_table(self, table_name: str) -> None:
        """创建新的KKV存储表"""
        try:
            self._validate_table_name(table_name)
            db_path = self._get_db_path(table_name)
            if os.path.exists(db_path):
                raise StoreError(f"Table already exists: {table_name}")

            # 创建表文件
            TinyDB(db_path).close()
            self._add_table_info(table_name, "kkv")
            logger.info(f"Created KKV table: {table_name}")
        except StoreError:
            raise
        except Exception as e:
            logger.error(f"Failed to create KKV table: {str(e)}")
            raise StoreError(f"Failed to create table: {str(e)}")

    def get_table(self, table_name: str) -> Union[TinyDBKVTable, TinyDBKKVTable]:
        """获取指定的存储表接口"""
        try:
            self._validate_table_name(table_name)
            table_type = self._get_table_type(table_name)
            db_path = self._get_db_path(table_name)

            if not os.path.exists(db_path):
                raise StoreError(f"Table file not found: {table_name}")

            if table_type == "kv":
                return TinyDBKVTable(table_name, db_path)
            elif table_type == "kkv":
                return TinyDBKKVTable(table_name, db_path)
            else:
                raise StoreError(f"Invalid table type: {table_type}")
        except StoreError:
            raise
        except Exception as e:
            logger.error(f"Failed to get table: {str(e)}")
            raise StoreError(f"Failed to get table: {str(e)}")

    def list_tables(self) -> Dict[str, str]:
        """获取所有表名列表"""
        try:
            table = self.db.table(self.TABLE_INFO_TABLE)
            return {doc["name"]: doc["type"] for doc in table.all() if "name" in doc}
        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            raise StoreError(f"Failed to list tables: {str(e)}")

    def drop_table(self, table_name: str) -> None:
        """删除指定的存储表"""
        try:
            self._validate_table_name(table_name)
            db_path = self._get_db_path(table_name)

            if not os.path.exists(db_path):
                raise StoreError(f"Table not found: {table_name}")

            # 关闭数据库连接
            if db_path in TinyDBTableBase._db_instances:
                TinyDBTableBase._db_instances[db_path].close()
                del TinyDBTableBase._db_instances[db_path]

            # 删除表文件
            os.remove(db_path)
            self._remove_table_info(table_name)
            logger.info(f"Dropped table: {table_name}")
        except StoreError:
            raise
        except Exception as e:
            logger.error(f"Failed to drop table: {str(e)}")
            raise StoreError(f"Failed to drop table: {str(e)}")
