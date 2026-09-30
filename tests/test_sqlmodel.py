"""SQLModel 公开接口测试。"""

import unittest

try:
    from sqlmodel import Field, Session, SQLModel, create_engine
except ModuleNotFoundError:
    Field = Session = SQLModel = create_engine = None

if SQLModel is not None:
    from funtable.sqlmodel import BaseModel

    class Item(BaseModel, table=True):
        """用于验证 ``BaseModel`` 行为的测试模型。"""

        name: str = Field(index=True)
        value: int = 0

        def unique_str(self) -> str:
            """使用名称生成稳定的唯一标识。"""
            return self.name


@unittest.skipIf(SQLModel is None, "sqlmodel extra is not installed")
class TestBaseModel(unittest.TestCase):
    """验证 SQLModel 后端的公开增删改查接口。"""

    def setUp(self) -> None:
        """为每个测试创建独立的内存数据库。"""
        self.engine = create_engine("sqlite://")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self) -> None:
        """关闭测试会话并释放数据库连接。"""
        self.session.close()
        self.engine.dispose()

    def test_create_query_and_unique_id(self) -> None:
        """创建后可按主键和唯一标识查询，且唯一标识保持稳定。"""
        item = Item.create({"name": "alpha", "value": 1}, self.session)

        self.assertIsNotNone(item)
        assert item is not None and item.id is not None and item.uid is not None
        self.assertEqual(Item.by_id(item.id, self.session), item)
        self.assertEqual(Item.by_uid(item.uid, self.session), item)
        self.assertEqual(Item.all(self.session), [item])
        self.assertEqual(
            Item(name="alpha", value=99).unique_str(), item.unique_str()
        )

    def test_upsert_commit_and_rollback_boundary(self) -> None:
        """upsert 服从提交参数，未提交修改可以由调用方回滚。"""
        pending = Item.upsert(
            {"name": "alpha", "value": 1}, self.session, commit=False
        )
        self.assertIsNotNone(pending)
        self.assertEqual(Item.all(self.session), [])

        committed = Item.upsert(
            {"name": "alpha", "value": 1}, self.session, commit=True
        )
        self.assertIsNotNone(committed)
        assert committed is not None
        Item.upsert({"name": "alpha", "value": 2}, self.session, commit=False)
        self.session.rollback()
        self.session.refresh(committed)
        self.assertEqual(committed.value, 1)

    def test_update_and_delete(self) -> None:
        """更新会持久化字段，删除会移除记录。"""
        item = Item.create({"name": "alpha", "value": 1}, self.session)
        assert item is not None and item.id is not None

        item.update({"name": "alpha", "value": 2}, self.session)
        self.assertEqual(Item.by_id(item.id, self.session).value, 2)

        item.delete(self.session)
        self.assertIsNone(Item.by_id(item.id, self.session))


if __name__ == "__main__":
    unittest.main()
