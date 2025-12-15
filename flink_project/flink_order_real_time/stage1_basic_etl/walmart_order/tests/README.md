# Flink JDBC MySQL Connection Test

## 概述

此测试用例用于验证 Flink 通过 JDBC 连接 MySQL 的功能，使用 JAR 文件进行连接。

## 测试内容

1. **创建测试数据库和表**：在 MySQL `testdb` schema 中创建 `flink_test_table` 表
2. **测试 Flink JDBC 连接**：使用 Flink JDBC connector 连接 MySQL
3. **加载测试数据**：将测试数据通过 Flink JDBC sink 写入 MySQL

## 测试表结构

```sql
CREATE TABLE flink_test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    value INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

## 使用的 JAR 文件

- `flink-connector-jdbc-3.1.1-1.17.jar` (位于 `jar/1.17/`)
- `mysql-connector-java-8.0.28.jar` (位于 `jar/`)

## 运行测试

```bash
cd /Users/hzz/KMS/flink-learning-practise/flink_project/flink_order_real_time/stage1_basic_etl/walmart_order/tests
conda activate py310
python test_flink_jdbc_mysql.py
```

## 测试步骤

1. **Step 1**: 创建测试表 `flink_test_table` 在 MySQL `testdb` schema
2. **Step 2**: 创建 Flink 执行环境
3. **Step 3**: 加载 JDBC 和 MySQL JAR 文件
4. **Step 4**: 配置 Flink 参数（并行度、checkpoint）
5. **Step 5**: 创建测试数据流（5 条测试记录）
6. **Step 6**: 构建 JDBC sink 并连接到 MySQL
7. **Step 7**: 执行 Flink 作业
8. **Step 8**: 验证数据是否成功写入 MySQL

## 预期结果

- 测试表 `flink_test_table` 中应该有 5 条记录
- 每条记录包含：name (test_item_1 到 test_item_5), value (100 到 500), created_at

## 日志文件

测试日志保存在：`/Users/hzz/KMS/flink-learning-practise/flink_project/logs/test_flink_jdbc_mysql.log`
