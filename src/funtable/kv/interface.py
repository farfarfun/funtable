"""
存储接口定义模块

本模块定义了KV(Key-Value)和KKV(Key-Key-Value)存储的抽象接口类。
包含了基础的存储操作接口定义和异常类定义。

Classes:
    StoreError: 存储操作异常类
    BaseKVTable: KV存储接口抽象类
    BaseKKVTable: KKV存储接口抽象类
    BaseDB: 数据库维度存储接口抽象类
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, TypeVar, Union

from funutil import getLogger

T = TypeVar("T", bound="BaseKVTable")
S = TypeVar("S", bound="BaseKKVTable")
logger = getLogger("funtable")


class StoreError(Exception):
    """存储操作异常类

    所有存储相关的错误都使用此异常类，具体错误类型通过错误信息区分。

    Attributes:
        message: 错误信息
        cause: 原始异常（如果有）
    """

    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:
        if self.cause:
            return f"{self.message} (Caused by: {str(self.cause)})"
        return self.message


class BaseKVTable(ABC):
    """表级KV(Key-Value)存储接口抽象类

    定义了基本的键值对存储操作接口，包括：
    - 设置键值对
    - 获取值
    - 删除键值对
    - 列出所有键
    - 列出所有键值对
    - 事务操作

    所有实现类都必须实现这些方法。
    """

    @abstractmethod
    def __init__(self, db_path: str, table_name: str):
        """初始化表级KV存储

        Args:
            db_path: 数据库文件路径
            table_name: 表名
        """
        pass

    @abstractmethod
    def set(self, key: str, value: Dict) -> None:
        """存储键值对

        Args:
            key: 字符串类型的键
            value: 字典类型的值

        Raises:
            StoreError: 当存储操作失败时抛出
        """
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[Dict]:
        """获取键的值

        Args:
            key: 要查询的键

        Returns:
            如果键存在，返回对应的字典值；如果不存在，返回None

        Raises:
            StoreError: 当查询操作失败时抛出
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """删除键值对

        Args:
            key: 要删除的键

        Returns:
            如果删除成功返回True，键不存在返回False

        Raises:
            StoreError: 当删除操作失败时抛出
        """
        pass

    @abstractmethod
    def list_keys(self) -> List[str]:
        """获取所有键列表

        Returns:
            包含所有键的列表

        Raises:
            StoreError: 当列举操作失败时抛出
        """
        pass

    @abstractmethod
    def list_all(self) -> Dict[str, Dict]:
        """获取所有键值对数据

        Returns:
            包含所有键值对的字典

        Raises:
            StoreError: 当查询操作失败时抛出
        """
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """开始事务

        Raises:
            StoreError: 当开始事务失败时抛出
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """提交事务

        Raises:
            StoreError: 当提交事务失败时抛出
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """回滚事务

        Raises:
            StoreError: 当回滚事务失败时抛出
        """
        pass


class BaseKKVTable(ABC):
    """表级KKV(Key-Key-Value)存储接口抽象类

    定义了两级键的键值对存储操作接口，包括：
    - 设置键值对
    - 获取值
    - 删除键值对
    - 列出所有主键
    - 列出指定主键下的所有次键
    - 列出所有键值对
    - 事务操作
    - 批量操作

    所有实现类都必须实现这些方法。
    """

    @abstractmethod
    def __init__(self, db_path: str, table_name: str):
        """初始化表级KKV存储

        Args:
            db_path: 数据库文件路径
            table_name: 表名
        """
        pass

    @abstractmethod
    def set(self, pkey: str, skey: str, value: Dict) -> None:
        """存储键值对

        Args:
            pkey: 第一级键
            skey: 第二级键
            value: 字典类型的值

        Raises:
            StoreError: 当存储操作失败时抛出
        """
        pass

    @abstractmethod
    def get(self, pkey: str, skey: str) -> Optional[Dict]:
        """获取键的值

        Args:
            pkey: 第一级键
            skey: 第二级键

        Returns:
            如果键存在，返回对应的字典值；如果不存在，返回None

        Raises:
            StoreError: 当查询操作失败时抛出
        """
        pass

    @abstractmethod
    def delete(self, pkey: str, skey: str) -> bool:
        """删除键值对

        Args:
            pkey: 第一级键
            skey: 第二级键

        Returns:
            如果删除成功返回True，键不存在返回False

        Raises:
            StoreError: 当删除操作失败时抛出
        """
        pass

    @abstractmethod
    def list_pkeys(self) -> List[str]:
        """获取所有第一级键列表

        Returns:
            包含所有第一级键的列表

        Raises:
            StoreError: 当列举操作失败时抛出
        """
        pass

    @abstractmethod
    def list_skeys(self, pkey: str) -> List[str]:
        """获取指定第一级键下的所有第二级键列表

        Args:
            pkey: 第一级键

        Returns:
            包含所有第二级键的列表

        Raises:
            StoreError: 当列举操作失败时抛出
        """
        pass

    @abstractmethod
    def list_all(self) -> Dict[str, Dict[str, Dict]]:
        """获取所有键值对数据

        Returns:
            包含所有键值对的字典，格式为 {key1: {key2: value_dict}}

        Raises:
            StoreError: 当查询操作失败时抛出
        """
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """开始事务

        Raises:
            StoreError: 当开始事务失败时抛出
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """提交事务

        Raises:
            StoreError: 当提交事务失败时抛出
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """回滚事务

        Raises:
            StoreError: 当回滚事务失败时抛出
        """
        pass

    @abstractmethod
    def batch_set(self, items: Dict[str, Dict[str, Dict]]) -> None:
        """批量设置键值对

        Args:
            items: 要设置的键值对字典，格式为 {pkey: {skey: value_dict}}

        Raises:
            StoreError: 当批量设置操作失败时抛出
        """
        pass

    @abstractmethod
    def batch_delete(self, items: List[tuple[str, str]]) -> None:
        """批量删除键值对

        Args:
            items: 要删除的键对列表，格式为 [(pkey, skey)]

        Raises:
            StoreError: 当批量删除操作失败时抛出
        """
        pass


class BaseDB(ABC):
    """数据库维度的存储接口抽象类

    定义了数据库级别的操作接口，包括：
    - 创建KV/KKV表
    - 获取表对象
    - 列出所有表
    - 删除表
    - 表信息管理
    """

    TABLE_INFO_TABLE = "_table_info"

    @abstractmethod
    def __init__(self, db_path: str):
        """初始化数据库维度存储

        Args:
            db_path: 数据库文件路径
        """
        pass

    @abstractmethod
    def create_kv_table(self, table_name: str) -> None:
        """创建新的KV表

        Args:
            table_name: 表名

        Raises:
            StoreError: 当表名不合法或表已存在时抛出
        """
        pass

    @abstractmethod
    def create_kkv_table(self, table_name: str) -> None:
        """创建新的KKV表

        Args:
            table_name: 表名

        Raises:
            StoreError: 当表名不合法或表已存在时抛出
        """
        pass

    @abstractmethod
    def get_table(self, table_name: str) -> Union[T, S]:
        """获取指定的表接口

        Args:
            table_name: 表名

        Returns:
            表级存储接口(KV或KKV)

        Raises:
            StoreError: 当表不存在或表名格式不合法时抛出
        """
        pass

    @abstractmethod
    def list_tables(self) -> Dict[str, str]:
        """获取所有表名列表

        Returns:
            表名和类型的映射字典，格式为 {table_name: "kv"|"kkv"}

        Raises:
            StoreError: 当列举操作失败时抛出
        """
        pass

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """删除指定的表

        Args:
            table_name: 表名

        Raises:
            StoreError: 当表不存在时抛出
        """
        pass
