# Changelog

## 未发布

### 修复

- 补全 `SQLiteStore`（`create_kv_table`/`create_kkv_table`/`get_table`/`list_tables`/`drop_table`）
  和 `TinyDBTableBase`（`begin_transaction`/`commit`/`rollback`）的中文 docstring。
- 修正 README：事务示例改为自带 KV 表初始化，不再复用前文 KKV 表导致签名不匹配；
  SQLModel 示例补上 `unique_str` 实现，使示例可实例化；快照示例改用
  `fundrive.get_drive("os")` 等实际存在的驱动，不再引用不存在的 `SomeDriveImplementation`；
  许可证徽章链接分支从 `main` 改为实际默认分支 `master`。
- `.gitignore` 补充 `*.db`、`*.rar`、`.run/`、`logs/`、`.vscode/`，覆盖测试产生的数据库文件等生成物。

## 1.0.47

### 新增

（无）

### 修复

- 将 LICENSE 正确收录到构建产物。
- 确保 TinyDB 建表时创建合法的空数据库文件，避免版本差异导致表不可访问。

### 变更

- 构建后端迁移至 Hatchling。
- 日志统一改用 `farlog`，类型语法和检查目标统一为 Python 3.10。
- 补齐云盘表与快照公开 API 的中文说明和类型标注。

### 废弃

（无）

## 1.0.46 及更早版本

早期版本未维护 CHANGELOG，具体变更参见 git 提交历史。
