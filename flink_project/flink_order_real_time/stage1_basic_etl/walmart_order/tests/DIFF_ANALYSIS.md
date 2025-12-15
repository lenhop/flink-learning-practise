# Flink JDBC MySQL 连接问题分析

## 问题描述

`flink6_walmart_order_pipeline.py` 无法将数据加载到 MySQL，而 `test_flink_jdbc_mysql.py` 可以成功加载数据。

## 关键差异对比

### 1. JDBC URL 配置（**最关键差异**）

#### test_flink_jdbc_mysql.py (成功) ✅
```python
.with_url(f"jdbc:mysql://{host}:{port}/testdb?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true")
```

#### flink6_walmart_order_pipeline.py (失败) ❌
```python
.with_url(f"jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}?useSSL=false&allowPublicKeyRetrieval=true")
```

**问题**: 缺少 `rewriteBatchedStatements=true` 参数

**影响**: 
- `rewriteBatchedStatements=true` 允许 MySQL JDBC 驱动将多个 INSERT 语句重写为单个批量 INSERT 语句
- 没有这个参数，Flink JDBC sink 可能无法正确执行批量插入操作
- 这是导致数据无法写入 MySQL 的主要原因

### 2. JDBC 执行选项差异

#### test_flink_jdbc_mysql.py (成功) ✅
```python
jdbc_exec_opts = JdbcExecutionOptions.builder() \
    .with_batch_size(10) \
    .with_batch_interval_ms(1000) \
    .with_max_retries(3) \
    .build()
```

#### flink6_walmart_order_pipeline.py (修复前) ❌
```python
jdbc_exec_opts = JdbcExecutionOptions.builder() \
    .with_batch_size(100) \
    .with_batch_interval_ms(5000) \
    .with_max_retries(3) \
    .build()
```

**差异**:
- batch_size: 100 vs 10 (测试用例使用更小的批次)
- batch_interval_ms: 5000 vs 1000 (测试用例使用更短的间隔)

**影响**: 
- 虽然批次大小和间隔不是根本原因，但更小的批次和更短的间隔可以提高可靠性
- 对于实时流处理，较小的批次可以更快地刷新数据

### 3. 数据流类型差异

#### test_flink_jdbc_mysql.py
- 使用 `env.from_collection()` 创建**有限数据流**（5条记录）
- 作业会在处理完所有数据后自动完成

#### flink6_walmart_order_pipeline.py
- 使用 Kafka source 创建**无限数据流**
- 作业会持续运行，等待新数据

**影响**: 
- 数据流类型不同不影响 JDBC sink 的写入功能
- 无限流也应该能够正常写入数据

### 4. SQL 语句差异

#### test_flink_jdbc_mysql.py
```sql
INSERT INTO flink_test_table (name, value, created_at) VALUES (?, ?, ?)
```

#### flink6_walmart_order_pipeline.py
```sql
REPLACE INTO walmart_order (...) VALUES (...)
```

**影响**: 
- `REPLACE INTO` vs `INSERT INTO` 不影响批量插入的执行
- 两种语句都支持批量操作

## 修复方案

### 已修复的问题

1. **添加 `rewriteBatchedStatements=true` 到 JDBC URL**
   ```python
   .with_url(f"jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true")
   ```

2. **调整批次参数以提高可靠性**
   ```python
   jdbc_exec_opts = JdbcExecutionOptions.builder() \
       .with_batch_size(50) \
       .with_batch_interval_ms(2000) \
       .with_max_retries(3) \
       .build()
   ```

## 根本原因总结

**主要问题**: JDBC URL 缺少 `rewriteBatchedStatements=true` 参数

这个参数对于 Flink JDBC sink 的批量插入操作至关重要：
- 没有它，MySQL JDBC 驱动可能无法正确处理批量 INSERT 语句
- Flink JDBC sink 依赖批量操作来提高性能和可靠性
- 缺少此参数可能导致数据在内存中缓冲但从未真正写入数据库

## 验证建议

修复后，建议：
1. 重新运行 `flink6_walmart_order_pipeline.py`
2. 向 Kafka 发送测试数据
3. 检查 MySQL 中是否有新数据写入
4. 监控 Flink 日志，确认没有错误信息
