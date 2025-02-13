"""
TinyDB存储实现模块

基于TinyDB实现KV和KKV存储接口。
TinyDB是一个轻量级的文档型数据库，数据以JSON格式存储。
"""

import os
import re
import time
from datetime import datetime
from typing import Dict, Optional, Union

from funutil import getLogger
from tinydb import Query, TinyDB

from .interface import (
    BaseDB,
    BaseKKVTable,
    BaseKVTable,
    StoreError,
)

logger = getLogger("funkv")


class TinyDBBase:
    """TinyDB数据库连接管理基类

    实现数据库连接的单例模式管理，确保同一数据库文件只创建一个连接实例。
    """

    _db_instances = {}  # 类变量，用于存储数据库实例的字典 {db_path: TinyDB实例}

    def __init__(self, db_path: str):
        """初始化TinyDB数据库连接

        Args:
            db_path: TinyDB数据库文件路径
        """
        self.db_path = db_path
        logger.debug(f"initializing TinyDB connection for {db_path}")

    @property
    def db(self) -> TinyDB:
        """获取数据库连接（单例模式）"""
        if self.db_path not in TinyDBBase._db_instances:
            logger.info(f"creating new TinyDB instance for {self.db_path}")
            TinyDBBase._db_instances[self.db_path] = TinyDB(self.db_path)
        return TinyDBBase._db_instances[self.db_path]

    def __del__(self):
        """析构函数，确保连接被正确关闭"""
        if self.db_path in TinyDBBase._db_instances:
            logger.debug(f"closing TinyDB connection for {self.db_path}")
            TinyDBBase._db_instances[self.db_path].close()
            del TinyDBBase._db_instances[self.db_path]


class TinyDBKVTable(TinyDBBase, BaseKVTable):
    """TinyDB的KV存储实现类

    使用TinyDB实现键值对存储，每个文档格式为:
    {
        "key": "键名",
        "value": {"key1": "value1", ...}  # 值必须是字典类型
    }
    """

    def __init__(self, db_path: str, table_name: str):
        super().__init__(db_path)
        self.table = self.db.table(table_name)
        self.query = Query()
        self._cache = {}  # 简单的内存缓存
        self._cache_ttl = 300  # 缓存时间 5 分钟

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
            self.table.upsert(
                {"key": key, "value": value},
                self.query.key == key,
            )
            self._cache[key] = (time.time(), value)
        except StoreError as e:
            logger.error(f"failed to set KV pair: {str(e)}")
            raise

    def get(self, key: str) -> Optional[Dict]:
        """获取键的值

        Args:
            key: 要查询的键

        Returns:
            如果键存在，返回对应的字典值；如果不存在，返回None
        """
        # 先查缓存
        if key in self._cache:
            cache_time, value = self._cache[key]
            if time.time() - cache_time < self._cache_ttl:
                return value
            del self._cache[key]

        # 查数据库
        logger.debug(f"getting value for key={key}")
        result = self.table.get(self.query.key == key)
        if result is not None:
            self._cache[key] = (time.time(), result["value"])
        return result["value"] if result else None

    def delete(self, key: str) -> bool:
        """删除键值对

        Args:
            key: 要删除的键

        Returns:
            如果删除成功返回True，键不存在返回False
        """
        logger.debug(f"deleting key={key}")
        # 删除缓存
        if key in self._cache:
            del self._cache[key]
        return bool(self.table.remove(self.query.key == key))

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

    def begin_transaction(self) -> None:
        """TinyDB 不支持真正的事务，这里只是为了兼容接口"""
        logger.warning("TinyDB does not support real transactions")

    def commit(self) -> None:
        """TinyDB 不支持真正的事务，这里只是为了兼容接口"""
        logger.warning("TinyDB does not support real transactions")

    def rollback(self) -> None:
        """TinyDB 不支持真正的事务，这里只是为了兼容接口"""
        logger.warning("TinyDB does not support real transactions")


class TinyDBKKVTable(TinyDBBase, BaseKKVTable):
    """TinyDB的KKV存储实现类

    使用TinyDB实现两级键的存储，每个文档格式为:
    {
        "key1": "主键名",
        "key2": "次键名",
        "value": {"key1": "value1", ...}  # 值必须是字典类型
    }
    """

    def __init__(self, db_path: str, table_name: str):
        super().__init__(db_path)
        self.table = self.db.table(table_name)
        self.query = Query()

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
            self.table.upsert(
                {"key1": pkey, "key2": skey, "value": value},
                (self.query.key1 == pkey) & (self.query.key2 == skey),
            )
        except StoreError as e:
            logger.error(f"failed to set KKV pair: {str(e)}")
            raise

    def get(self, pkey: str, skey: str) -> Optional[Dict]:
        """获取键的值

        Args:
            pkey: 第一级键
            skey: 第二级键

        Returns:
            如果键存在，返回对应的字典值；如果不存在，返回None
        """
        logger.debug(f"getting value for pkey={pkey}, skey={skey}")
        result = self.table.get((self.query.key1 == pkey) & (self.query.key2 == skey))
        return result["value"] if result else None

    def delete(self, pkey: str, skey: str) -> bool:
        """删除键值对

        Args:
            pkey: 第一级键
            skey: 第二级键

        Returns:
            如果删除成功返回True，键不存在返回False
        """
        logger.debug(f"deleting pkey={pkey}, skey={skey}")
        return bool(
            self.table.remove((self.query.key1 == pkey) & (self.query.key2 == skey))
        )

    def list_pkeys(self) -> list[str]:
        """获取所有第一级键列表

        Returns:
            包含所有第一级键的列表
        """
        keys = {doc["key1"] for doc in self.table.all()}
        return list(keys)

    def list_skeys(self, pkey: str) -> list[str]:
        """获取指定第一级键下的所有第二级键列表

        Args:
            pkey: 第一级键

        Returns:
            包含所有第二级键的列表
        """
        return [doc["key2"] for doc in self.table.search(self.query.key1 == pkey)]

    def list_all(self) -> Dict[str, Dict[str, Dict]]:
        """获取所有键值对数据

        Returns:
            包含所有键值对的字典，格式为 {key1: {key2: value_dict}}
        """
        result: Dict[str, Dict[str, Dict]] = {}
        for doc in self.table.all():
            pkey = doc["key1"]
            skey = doc["key2"]
            if pkey not in result:
                result[pkey] = {}
            result[pkey][skey] = doc["value"]
        return result

    def begin_transaction(self) -> None:
        logger.warning("TinyDB does not support real transactions")

    def commit(self) -> None:
        logger.warning("TinyDB does not support real transactions")

    def rollback(self) -> None:
        logger.warning("TinyDB does not support real transactions")


class TinyDBStore(TinyDBBase, BaseDB):
    """TinyDB数据库维度存储实现"""

    def __init__(self, db_dir: str = "tinydb_store"):
        logger.info(f"Initializing TinyDBStore in directory: {db_dir}")
        self.db_dir = db_dir
        os.makedirs(db_dir, exist_ok=True)
        self._table_name_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
        self._table_info_path = os.path.join(db_dir, ".table_info")
        super().__init__(self._table_info_path)
        self._init_table_info_table()

    def _init_table_info_table(self) -> None:
        """初始化存储表信息表"""
        if not os.path.exists(self._table_info_path):
            db = TinyDB(self._table_info_path)
            db.close()

    def _add_table_info(self, table_name: str, table_type: str) -> None:
        self.db.table(self.TABLE_INFO_TABLE).upsert(
            {
                "name": table_name,
                "type": table_type,
                "created_at": str(datetime.now()),
                "updated_at": str(datetime.now()),
            },
            Query().name == table_name,
        )

    def _remove_table_info(self, table_name: str) -> None:
        self.db.table(self.TABLE_INFO_TABLE).remove(Query().name == table_name)

    def _get_table_type(self, table_name: str) -> str:
        result = self.db.table(self.TABLE_INFO_TABLE).get(Query().name == table_name)
        if result is None:
            raise StoreError(f"table '{table_name}' does not exist")
        return result["type"]

    def _ensure_table_exists(self, table_name: str) -> None:
        """确保表存在"""
        if not self.db.table(self.TABLE_INFO_TABLE).contains(
            Query().name == table_name
        ):
            raise StoreError(f"Table '{table_name}' does not exist")
        db_path = self._get_db_path(table_name)
        if not os.path.exists(db_path):
            raise StoreError(f"Table '{table_name}' database file does not exist")

    def _get_db_path(self, table_name: str) -> str:
        """获取存储表的数据库文件路径"""
        return os.path.join(self.db_dir, f"{table_name}.json")

    def _validate_table_name(self, table_name: str) -> None:
        """验证表名是否合法"""
        if not isinstance(table_name, str):
            raise StoreError("Table name must be string type")
        if not self._table_name_pattern.match(table_name):
            raise StoreError(
                "Table name must start with a letter and contain only letters, numbers and underscores"
            )

    def get_table(self, table_name: str) -> Union[TinyDBKVTable, TinyDBKKVTable]:
        """获取指定的存储表接口"""
        self._ensure_table_exists(table_name)
        table_type = self._get_table_type(table_name)
        if table_type == "kkv":
            return TinyDBKKVTable(self._get_db_path(table_name), table_name)
        return TinyDBKVTable(self._get_db_path(table_name), table_name)

    def list_tables(self) -> Dict[str, str]:
        return {
            doc["name"]: doc["type"]
            for doc in self.db.table(self.TABLE_INFO_TABLE).all()
        }

    def create_kv_table(self, table_name: str) -> None:
        """创建新的KV存储表"""
        try:
            self._validate_table_name(table_name)
            logger.info(f"creating KV table: {table_name}")
            db_path = self._get_db_path(table_name)
            # 创建空表并记录类型
            db = TinyDB(db_path)
            db.close()
            self._add_table_info(table_name, "kv")
            logger.success(f"created KV table: {table_name} success")
        except StoreError as e:
            logger.error(f"Failed to create KV table {table_name}: {str(e)}")
            raise

    def create_kkv_table(self, table_name: str) -> None:
        """创建新的KKV存储表"""
        self._validate_table_name(table_name)
        db_path = self._get_db_path(table_name)
        # 创建空表并记录类型
        db = TinyDB(db_path)
        db.close()
        self._add_table_info(table_name, "kkv")
        logger.success(f"created KKV table: {table_name} success")

    def drop_table(self, table_name: str) -> None:
        """删除指定的存储表"""
        try:
            self._ensure_table_exists(table_name)
            logger.info(f"dropping table: {table_name}")
            db_path = self._get_db_path(table_name)
            if os.path.exists(db_path):
                os.remove(db_path)
            self._remove_table_info(table_name)
        except StoreError as e:
            logger.error(f"failed to drop table {table_name}: {str(e)}")
            raise
