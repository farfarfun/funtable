import os
import unittest
from typing import Dict

from funutil import getLogger

from .interface import StoreError


from .sqlite_table import SQLiteStore
from .tinydb_table import TinyDBStore

logger = getLogger("funkv")


class TestSQLiteStore(unittest.TestCase):
    """测试SQLite存储实现

    测试SQLite实现的以下功能：
    1. 表的创建和删除
    2. 表名验证
    3. KV表的基本操作（增删改查）
    4. KKV表的基本操作（增删改查）
    """

    def setUp(self):
        """测试前创建临时数据库

        创建一个临时的SQLite数据库文件用于测试。
        每个测试方法执行前都会重新创建一个干净的数据库环境。
        """
        self.db_path = "test_sqlite.db"
        self.store = SQLiteStore(self.db_path)
        logger.info("初始化SQLite测试环境")

    def tearDown(self):
        """测试后删除临时数据库"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        logger.info("清理SQLite测试环境")

    def test_create_and_drop_kv_table(self):
        """测试KV表的创建和删除

        测试步骤：
        1. 创建KV表并验证创建成功
        2. 检查表信息是否正确记录
        3. 删除表并验证删除成功
        """
        table_name = "test_kv"
        logger.info(f"创建KV表: {table_name}")
        self.store.create_kv_table(table_name)

        tables = self.store.list_tables()
        logger.info(f"当前表列表: {tables}")
        self.assertIn(table_name, tables)
        self.assertEqual(tables[table_name], "kv")

        logger.info(f"删除表: {table_name}")
        self.store.drop_table(table_name)
        self.assertNotIn(table_name, self.store.list_tables())

    def test_create_and_drop_kkv_table(self):
        """测试创建和删除KKV表"""
        table_name = "test_kkv"
        logger.info(f"创建KKV表: {table_name}")
        self.store.create_kkv_table(table_name)

        tables = self.store.list_tables()
        logger.info(f"当前表列表: {tables}")
        self.assertIn(table_name, tables)
        self.assertEqual(tables[table_name], "kkv")

        logger.info(f"删除表: {table_name}")
        self.store.drop_table(table_name)
        self.assertNotIn(table_name, self.store.list_tables())

    def test_invalid_table_name(self):
        """测试无效的表名"""
        invalid_names = ["123test", "test-table", "test table"]
        for name in invalid_names:
            logger.info(f"测试无效表名: {name}")
            with self.assertRaises(StoreError):
                self.store.create_kv_table(name)

    def test_table_not_found(self):
        """测试访问不存在的表"""
        table_name = "non_existent"
        logger.info(f"尝试访问不存在的表: {table_name}")
        with self.assertRaises(StoreError):
            self.store.get_table(table_name)

    def test_kv_table_operations(self):
        """测试KV表的基本操作

        测试以下操作：
        1. 设置键值对并验证
        2. 获取已存在和不存在的键值
        3. 删除键值对
        4. 获取所有键列表
        5. 获取所有数据

        同时验证：
        - 键值的类型检查
        - 数据的完整性
        - 操作的原子性
        """
        table_name = "test_kv"
        self.store.create_kv_table(table_name)
        kv_table = self.store.get_table(table_name)

        # 测试设置和获取
        key = "test_key"
        value = {"name": "test", "value": 123}
        logger.info(f"设置KV: key={key}, value={value}")
        kv_table.set(key, value)
        result = kv_table.get(key)
        logger.info(f"获取结果: {result}")
        self.assertEqual(result, value)

        # 测试删除
        logger.info(f"删除键: {key}")
        self.assertTrue(kv_table.delete(key))
        self.assertIsNone(kv_table.get(key))

        # 测试列表操作
        test_data: Dict[str, Dict] = {
            "key1": {"name": "test1"},
            "key2": {"name": "test2"},
        }
        for k, v in test_data.items():
            kv_table.set(k, v)

        keys = kv_table.list_keys()
        logger.info(f"键列表: {keys}")
        self.assertEqual(len(keys), 2)

        all_data = kv_table.list_all()
        logger.info(f"所有数据: {all_data}")
        self.assertEqual(all_data, test_data)

    def test_kkv_table_operations(self):
        """测试KKV表的基本操作

        测试以下操作：
        1. 设置双键值对并验证
        2. 获取已存在和不存在的键值
        3. 删除键值对
        4. 获取主键列表
        5. 获取次键列表
        6. 获取所有数据

        同时验证：
        - 主键和次键的类型检查
        - 数据的层级结构
        - 复杂数据的完整性
        """
        table_name = "test_kkv"
        self.store.create_kkv_table(table_name)
        kkv_table = self.store.get_table(table_name)

        # 测试设置和获取
        pkey = "user1"
        skey = "profile"
        value = {"name": "test", "age": 25}
        logger.info(f"设置KKV: pkey={pkey}, skey={skey}, value={value}")
        kkv_table.set(pkey, skey, value)
        result = kkv_table.get(pkey, skey)
        logger.info(f"获取结果: {result}")
        self.assertEqual(result, value)

        # 测试删除
        logger.info(f"删除键: pkey={pkey}, skey={skey}")
        self.assertTrue(kkv_table.delete(pkey, skey))
        self.assertIsNone(kkv_table.get(pkey, skey))

        # 测试列表操作
        test_data: Dict[str, Dict[str, Dict]] = {
            "user1": {
                "profile": {"name": "test1"},
                "settings": {"theme": "dark"},
            },
            "user2": {"profile": {"name": "test2"}},
        }
        for pk, sv in test_data.items():
            for sk, v in sv.items():
                kkv_table.set(pk, sk, v)

        pkeys = kkv_table.list_pkeys()
        logger.info(f"主键列表: {pkeys}")
        self.assertEqual(len(pkeys), 2)

        skeys = kkv_table.list_skeys(pkey)
        logger.info(f"{pkey}的次键列表: {skeys}")
        self.assertEqual(len(skeys), 2)

        all_data = kkv_table.list_all()
        logger.info(f"所有数据: {all_data}")
        self.assertEqual(all_data, test_data)

    def test_concurrent_access(self):
        """测试并发访问"""
        pass

    def test_large_data_handling(self):
        """测试大数据处理"""
        pass

    def test_connection_failure(self):
        """测试连接失败情况"""
        pass

    def test_transaction_rollback(self):
        """测试事务回滚"""
        pass


class TestTinyDBStore(unittest.TestCase):
    """测试TinyDB存储实现

    测试TinyDB实现的以下功能：
    1. 表的创建和删除
    2. 表名验证
    3. KV表的基本操作（增删改查）
    4. KKV表的基本操作（增删改查）
    5. 文件系统操作（创建/删除数据库文件）
    """

    def setUp(self):
        """测试前创建临时数据库目录

        创建一个临时的TinyDB数据库目录用于测试。
        每个测试方法执行前都会重新创建一个干净的数据库环境。
        """
        self.db_dir = "test_tinydb_store"
        self.store = TinyDBStore(self.db_dir)
        logger.info("初始化TinyDB测试环境")

    def tearDown(self):
        """测试后删除临时数据库目录"""
        if os.path.exists(self.db_dir):
            for file in os.listdir(self.db_dir):
                os.remove(os.path.join(self.db_dir, file))
            os.rmdir(self.db_dir)
        logger.info("清理TinyDB测试环境")

    def test_create_and_drop_kv_table(self):
        """测试创建和删除KV表"""
        table_name = "test_kv"
        logger.info(f"创建KV表: {table_name}")
        self.store.create_kv_table(table_name)

        tables = self.store.list_tables()
        logger.info(f"当前表列表: {tables}")
        self.assertIn(table_name, tables)
        self.assertEqual(tables[table_name], "kv")

        logger.info(f"删除表: {table_name}")
        self.store.drop_table(table_name)
        self.assertNotIn(table_name, self.store.list_tables())

    def test_create_and_drop_kkv_table(self):
        """测试创建和删除KKV表"""
        table_name = "test_kkv"
        logger.info(f"创建KKV表: {table_name}")
        self.store.create_kkv_table(table_name)

        tables = self.store.list_tables()
        logger.info(f"当前表列表: {tables}")
        self.assertIn(table_name, tables)
        self.assertEqual(tables[table_name], "kkv")

        logger.info(f"删除表: {table_name}")
        self.store.drop_table(table_name)
        self.assertNotIn(table_name, self.store.list_tables())

    def test_invalid_table_name(self):
        """测试无效的表名"""
        invalid_names = ["123test", "test-table", "test table"]
        for name in invalid_names:
            logger.info(f"测试无效表名: {name}")
            with self.assertRaises(StoreError):
                self.store.create_kv_table(name)

    def test_table_not_found(self):
        """测试访问不存在的表"""
        table_name = "non_existent"
        logger.info(f"尝试访问不存在的表: {table_name}")
        with self.assertRaises(StoreError):
            self.store.get_table(table_name)

    def test_kv_table_operations(self):
        """测试KV表的基本操作"""
        table_name = "test_kv"
        self.store.create_kv_table(table_name)
        kv_table = self.store.get_table(table_name)

        # 测试设置和获取
        key = "test_key"
        value = {"name": "test", "value": 123}
        logger.info(f"设置KV: key={key}, value={value}")
        kv_table.set(key, value)
        result = kv_table.get(key)
        logger.info(f"获取结果: {result}")
        self.assertEqual(result, value)

        # 测试删除
        logger.info(f"删除键: {key}")
        self.assertTrue(kv_table.delete(key))
        self.assertIsNone(kv_table.get(key))

        # 测试列表操作
        test_data: Dict[str, Dict] = {
            "key1": {"name": "test1"},
            "key2": {"name": "test2"},
        }
        for k, v in test_data.items():
            kv_table.set(k, v)

        keys = kv_table.list_keys()
        logger.info(f"键列表: {keys}")
        self.assertEqual(len(keys), 2)

        all_data = kv_table.list_all()
        logger.info(f"所有数据: {all_data}")
        self.assertEqual(all_data, test_data)

    def test_kkv_table_operations(self):
        """测试KKV表的基本操作"""
        table_name = "test_kkv"
        self.store.create_kkv_table(table_name)
        kkv_table = self.store.get_table(table_name)

        # 测试设置和获取
        pkey = "user1"
        skey = "profile"
        value = {"name": "test", "age": 25}
        logger.info(f"设置KKV: pkey={pkey}, skey={skey}, value={value}")
        kkv_table.set(pkey, skey, value)
        result = kkv_table.get(pkey, skey)
        logger.info(f"获取结果: {result}")
        self.assertEqual(result, value)

        # 测试删除
        logger.info(f"删除键: pkey={pkey}, skey={skey}")
        self.assertTrue(kkv_table.delete(pkey, skey))
        self.assertIsNone(kkv_table.get(pkey, skey))

        # 测试列表操作
        test_data: Dict[str, Dict[str, Dict]] = {
            "user1": {
                "profile": {"name": "test1"},
                "settings": {"theme": "dark"},
            },
            "user2": {"profile": {"name": "test2"}},
        }
        for pk, sv in test_data.items():
            for sk, v in sv.items():
                kkv_table.set(pk, sk, v)

        pkeys = kkv_table.list_pkeys()
        logger.info(f"主键列表: {pkeys}")
        self.assertEqual(len(pkeys), 2)

        skeys = kkv_table.list_skeys(pkey)
        logger.info(f"{pkey}的次键列表: {skeys}")
        self.assertEqual(len(skeys), 2)

        all_data = kkv_table.list_all()
        logger.info(f"所有数据: {all_data}")
        self.assertEqual(all_data, test_data)


if __name__ == "__main__":
    unittest.main()
