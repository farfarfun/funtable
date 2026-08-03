# FunTable

[![PyPI version](https://badge.fury.io/py/funtable.svg)](https://badge.fury.io/py/funtable)
[![Python](https://img.shields.io/pypi/pyversions/funtable.svg)](https://pypi.org/project/funtable/)
[![License](https://img.shields.io/github/license/farfarfun/funtable.svg)](https://github.com/farfarfun/funtable/blob/main/LICENSE)

一个统一的表存储抽象库，为各种存储后端（包括 SQLite、TinyDB、SQLModel 和云存储快照）提供一致的接口。

## 特性

- **统一接口**: 跨不同存储后端的一致 API
- **多种存储类型**: 支持 KV（键值）和 KKV（键键值）存储模式
- **事务支持**: SQLite 后端提供事务管理，确保数据一致性
- **多种后端**: 支持 SQLite、TinyDB、SQLModel 和云存储
- **快照管理**: 自动备份和版本管理，支持云存储
- **线程安全**: 为并发访问场景而设计

## 安装

### 基础安装

```bash
pip install funtable
```

### 安装可选依赖

```bash
# 支持 SQLModel
pip install funtable[sqlmodel]

# 支持快照/云存储
pip install funtable[snapshot]

# 支持 TinyDB
pip install funtable[kv]

# 安装所有可选依赖
pip install funtable[sqlmodel,snapshot,kv]
```

## 快速开始

### KV（键值）存储

```python
from funtable.kv import SQLiteStore

# 初始化数据库
db = SQLiteStore("my_database.db")

# 创建 KV 表
db.create_kv_table("users")
table = db.get_table("users")

# 存储数据
table.set("user1", {"name": "Alice", "age": 30})
table.set("user2", {"name": "Bob", "age": 25})

# 检索数据
user = table.get("user1")
print(user)  # {"name": "Alice", "age": 30}

# 列出所有键
keys = table.list_keys()
print(keys)  # ["user1", "user2"]

# 获取所有数据
all_data = table.list_all()
print(all_data)
```

### KKV（键键值）存储

```python
from funtable.kv import SQLiteStore

# 初始化数据库
db = SQLiteStore("my_database.db")

# 创建 KKV 表
db.create_kkv_table("user_profiles")
table = db.get_table("user_profiles")

# 使用两级键存储数据
table.set("user1", "profile", {"bio": "Software Engineer"})
table.set("user1", "settings", {"theme": "dark", "notifications": True})
table.set("user2", "profile", {"bio": "Data Scientist"})

# 检索数据
profile = table.get("user1", "profile")
print(profile)  # {"bio": "Software Engineer"}

# 列出主键
pkeys = table.list_pkeys()
print(pkeys)  # ["user1", "user2"]

# 列出某个主键的次键
skeys = table.list_skeys("user1")
print(skeys)  # ["profile", "settings"]
```

### 事务支持

事务仅适用于 SQLite 后端。

```python
# 使用事务确保数据一致性
table.begin_transaction()
try:
    table.set("user3", {"name": "Charlie", "age": 35})
    table.set("user4", {"name": "David", "age": 40})
    table.commit()
except Exception as e:
    table.rollback()
    print(f"事务失败: {e}")
```

### SQLModel 集成

```python
from funtable.sqlmodel import BaseModel
from sqlmodel import Field
from datetime import datetime


class User(BaseModel, table=True):
    name: str = Field(description="用户名")
    email: str = Field(description="用户邮箱")
    age: int = Field(description="用户年龄")


# BaseModel 提供内置字段:
# - id: 自增主键
# - uid: 唯一标识符
# - gmt_create: 创建时间戳
# - gmt_modified: 修改时间戳

# 与 SQLModel session 一起使用
from sqlmodel import create_engine, Session

engine = create_engine("sqlite:///users.db")
with Session(engine) as session:
    user = User(name="Alice", email="alice@example.com", age=30)
    session.add(user)
    session.commit()

    # 查询方法
    all_users = User.all(session)
    user_by_id = User.by_id(1, session)
    user_by_uid = User.by_uid("some-uid", session)
```

### 快照管理

```python
from funtable.snapshot import DriveSnapshot
from fundrive import SomeDriveImplementation

# 初始化驱动器和快照管理器
drive = SomeDriveImplementation()
snapshot = DriveSnapshot(
    table_fid="my-table-id",
    drive=drive,
    num=7,  # 保留 7 个版本
)

# 创建快照
snapshot.update("/path/to/data", partition="20231201")

# 下载最新快照
snapshot.download("/path/to/restore")
```

## 存储后端

### SQLite 后端
- **文件**: `funtable.kv.sqlite_table`
- **特性**: ACID 事务、SQL 查询、基于文件的存储
- **适用于**: 生产应用、复杂查询、数据完整性

### TinyDB 后端
- **文件**: `funtable.kv.tinydb_table`
- **特性**: 基于 JSON、轻量级文件存储
- **适用于**: 小型应用、原型开发、简单数据结构

### SQLModel 集成
- **文件**: `funtable.sqlmodel.base`
- **特性**: ORM 功能、类型安全、Pydantic 集成
- **适用于**: 现代 Python 应用、API 开发、数据验证

### 云存储快照
- **文件**: `funtable.snapshot.core`
- **特性**: 自动备份、版本管理、云集成
- **适用于**: 数据备份、灾难恢复、分布式系统

## API 参考

### 核心接口

#### BaseKVTable
- `set(key: str, value: Dict) -> None`: 存储键值对
- `get(key: str) -> Optional[Dict]`: 通过键检索值
- `delete(key: str) -> bool`: 删除键值对
- `list_keys() -> List[str]`: 获取所有键
- `list_all() -> Dict[str, Dict]`: 获取所有键值对
- `begin_transaction()`, `commit()`, `rollback()`: 事务管理

#### BaseKKVTable
- `set(pkey: str, skey: str, value: Dict) -> None`: 使用两级键存储
- `get(pkey: str, skey: str) -> Optional[Dict]`: 通过两级键检索
- `delete(pkey: str, skey: str) -> bool`: 通过两级键删除
- `list_pkeys() -> List[str]`: 获取所有主键
- `list_skeys(pkey: str) -> List[str]`: 获取主键的次键
- `list_all() -> Dict[str, Dict[str, Dict]]`: 获取所有数据
- `batch_set(items: Dict) -> None`: 批量插入操作
- `batch_delete(items: List[tuple]) -> int`: 批量删除操作，返回删除数量

#### BaseDB
- `create_kv_table(table_name: str) -> None`: 创建 KV 表
- `create_kkv_table(table_name: str) -> None`: 创建 KKV 表
- `get_table(table_name: str) -> Union[BaseKVTable, BaseKKVTable]`: 获取表实例
- `list_tables() -> Dict[str, str]`: 列出所有表及其类型
- `drop_table(table_name: str) -> None`: 删除表

## 错误处理

所有存储操作都可能引发 `StoreError` 异常：

```python
from funtable.kv.interface import StoreError

try:
    table.set("key", {"data": "value"})
except StoreError as e:
    print(f"存储错误: {e}")
    if e.cause:
        print(f"原因: {e.cause}")
```

## 贡献

1. Fork 仓库
2. 创建功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开 Pull Request

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 链接

- **仓库**: https://github.com/farfarfun/funtable
- **PyPI**: https://pypi.org/project/funtable/
- **问题**: https://github.com/farfarfun/funtable/issues
- **发布**: https://github.com/farfarfun/funtable/releases
