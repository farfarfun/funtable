"""
存储接口定义模块

本模块定义了KV(Key-Value)和KKV(Key-Key-Value)存储的抽象接口类。
包含了基础的存储操作接口定义和异常类定义。
"""

from abc import ABC, abstractmethod
from logging import getLogger
from typing import Dict, NewType, Optional, TypedDict, TypeVar, Union

T = TypeVar("T", bound="BaseKVTable")
S = TypeVar("S", bound="BaseKKVTable")

StoreKey = NewType("StoreKey", str)  # 存储键类型，基于字符串


class StoreValueDict(TypedDict):
    """存储值字典类型"""

    created_at: float
    updated_at: float
    data: Dict[str, any]


StoreValue = StoreValueDict  # 更严格的值类型定义

StoreName = NewType("StoreName", str)  # 存储表名类型，基于字符串


class StoreError(Exception):
    """存储操作基础异常类

    所有存储相关的自定义异常都应继承自此类
    """

    pass


class KeyError(StoreError):
    """键错误异常

    当操作的键类型不正确时抛出此异常
    """

    pass


class StoreValueError(StoreError):
    """值错误异常

    当操作的值类型不正确或格式不合法时抛出此异常
    """

    pass


class StoreNotFoundError(StoreError):
    """存储表不存在异常

    当操作的存储表不存在时抛出此异常
    """

    pass


class TableExistsError(StoreError):
    """表已存在异常"""

    pass


class TableNameError(StoreError):
    """表名格式错误异常"""

    pass


class ConnectionError(StoreError):
    """数据库连接异常"""

    pass


class BaseKVTable(ABC):
    """表级KV(Key-Value)存储接口抽象类

    定义了基本的键值对存储操作接口，包括：
    - 设置键值对
    - 获取值
    - 删除键值对
    - 列出所有键
    - 列出所有键值对
    """

    @abstractmethod
    def __init__(self, db_path: str, table_name: str):
        """初始化表级KV存储

        Args:
            db_path: 数据库文件路径
            table_name: 表名
        """
        pass  # 移除具体实现，保持抽象

    @abstractmethod
    def set(self, key: StoreKey, value: StoreValue) -> None:
        """存储键值对

        Args:
            key: 字符串类型的键
            value: 字典类型的值

        Raises:
            KeyError: 当键不是字符串类型时
            StoreValueError: 当值不是字典类型时
        """
        pass

    @abstractmethod
    def get(self, key: StoreKey) -> Optional[StoreValue]:
        """获取键的值

        Args:
            key: 要查询的键

        Returns:
            如果键存在，返回对应的字典值；如果不存在，返回None
        """
        pass

    @abstractmethod
    def delete(self, key: StoreKey) -> bool:
        """删除键值对

        Args:
            key: 要删除的键

        Returns:
            如果删除成功返回True，键不存在返回False
        """
        pass

    @abstractmethod
    def list_keys(self) -> list[StoreKey]:
        """获取所有键列表

        Returns:
            包含所有键的列表
        """
        pass

    @abstractmethod
    def list_all(self) -> Dict[StoreKey, StoreValue]:
        """获取所有键值对数据

        Returns:
            包含所有键值对的字典，格式为 {key: value_dict}
        """
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """开始事务"""
        pass

    @abstractmethod
    def commit(self) -> None:
        """提交事务"""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """回滚事务"""
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
    """

    @abstractmethod
    def __init__(self, db_path: str, table_name: str):
        """初始化表级KKV存储

        Args:
            db_path: 数据库文件路径
            table_name: 表名
        """
        pass  # 移除具体实现，保持抽象

    @abstractmethod
    def set(self, pkey: StoreKey, skey: StoreKey, value: StoreValue) -> None:
        """存储键值对

        Args:
            pkey: 主键
            skey: 次键
            value: 字典类型的值
        """
        pass

    @abstractmethod
    def get(self, pkey: StoreKey, skey: StoreKey) -> Optional[StoreValue]:
        """获取键的值

        Args:
            pkey: 主键
            skey: 次键

        Returns:
            如果键存在，返回对应的字典值；如果不存在，返回None
        """
        pass

    @abstractmethod
    def delete(self, pkey: StoreKey, skey: StoreKey) -> bool:
        """删除键值对

        Args:
            pkey: 主键
            skey: 次键

        Returns:
            如果删除成功返回True，键不存在返回False
        """
        pass

    @abstractmethod
    def list_pkeys(self) -> list[StoreKey]:
        """获取所有主键列表

        Returns:
            包含所有主键的列表
        """
        pass

    @abstractmethod
    def list_skeys(self, pkey: StoreKey) -> list[StoreKey]:
        """获取指定主键下的所有次键列表

        Args:
            pkey: 主键

        Returns:
            包含所有次键的列表
        """
        pass

    @abstractmethod
    def list_all(self) -> Dict[StoreKey, Dict[StoreKey, StoreValue]]:
        """获取所有键值对数据

        Returns:
            包含所有键值对的字典，格式为 {pkey: {skey: value_dict}}
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

    TABLE_INFO_TABLE = "_table_info"  # 存储表信息的特殊表名

    def __init__(self):
        self.logger = getLogger(f"{self.__class__.__name__}")

    @abstractmethod
    def _init_table_info_table(self) -> None:
        """初始化表信息管理表"""
        pass

    @abstractmethod
    def _add_table_info(self, table_name: StoreName, table_type: str) -> None:
        """添加或更新表信息

        Args:
            table_name: 表名
            table_type: 表类型 ("kv" 或 "kkv")
        """
        pass

    @abstractmethod
    def _remove_table_info(self, table_name: StoreName) -> None:
        """删除表信息

        Args:
            table_name: 表名
        """
        pass

    @abstractmethod
    def _get_table_type(self, table_name: StoreName) -> str:
        """获取表类型

        Args:
            table_name: 表名

        Returns:
            表类型 ("kv" 或 "kkv")

        Raises:
            StoreNotFoundError: 当表不存在时
        """
        pass

    @abstractmethod
    def create_kv_table(self, table_name: StoreName) -> None:
        """创建新的KV表

        Args:
            table_name: 表名

        Raises:
            StoreValueError: 当表名不合法时抛出
        """
        self.logger.info(f"Creating KV table: {table_name}")
        try:
            # 创建表逻辑
            self.logger.debug("Table created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}", exc_info=True)
            raise

    @abstractmethod
    def create_kkv_table(self, table_name: StoreName) -> None:
        """创建新的KKV表"""
        self.logger.info(f"Creating KKV table: {table_name}")
        try:
            # 创建表逻辑
            self.logger.debug("Table created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}", exc_info=True)
            raise

    @abstractmethod
    def get_table(self, table_name: StoreName) -> Union[BaseKVTable, BaseKKVTable]:
        """获取指定的表接口

        Args:
            table_name: 表名

        Returns:
            表级存储接口(KV或KKV)

        Raises:
            StoreNotFoundError: 当表不存在时
            StoreValueError: 当表名格式不合法时
        """
        pass

    @abstractmethod
    def list_tables(self) -> Dict[StoreName, str]:
        """获取所有表名列表

        Returns:
            表名和类型的映射字典，格式为 {table_name: "kv"|"kkv"}
        """
        pass

    @abstractmethod
    def drop_table(self, table_name: StoreName) -> None:
        """删除指定的表

        Args:
            table_name: 表名

        Raises:
            StoreValueError: 当表不存在时抛出
        """
        pass
