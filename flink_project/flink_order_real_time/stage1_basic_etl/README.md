# Stage1 Basic ETL

## 1. 项目概述

本项目实现 Walmart 订单数据的完整 ETL 流程：**API 获取 → Kafka → Flink 处理 → MySQL 存储**。

主要功能模块：
- **订单获取**：从 Walmart API 获取订单数据并推送到 Kafka
- **数据解析**：解析 Walmart 订单 JSON 数据，提取订单头和订单行信息
- **流式处理**：使用 Flink 从 Kafka 消费数据，解析后写入 MySQL
- **数据库初始化**：创建 ODS 层数据表

数据流向：
```
Walmart API → Kafka (walmart_order_raw) → Flink → MySQL (ods.walmart_order)
```

## 2. 使用步骤

### 2.1 环境准备

确保以下服务已启动：
- Kafka 集群（默认：`localhost:29092`, `localhost:39092`）
- MySQL 数据库（配置见 `config/config.yml`）
- Flink 集群（用于流式处理）

### 2.2 初始化数据库

```bash
# 创建数据库和表
cd init_database
python init_database_env.py
```

### 2.3 获取订单并推送到 Kafka

```bash
cd walmart_order
python request_order_and_push_kafka.py
```

**说明**：
- 脚本会从 Walmart API 获取订单数据
- 自动创建 Kafka topic `walmart_order_raw`（如果不存在）
- 将订单数据批量推送到 Kafka（每批 20 条）
- 支持单日或日期范围的数据获取

**配置参数**（在脚本中修改）：
```python
# 处理日期范围
start_date_str = "2025-10-01"
end_date_str = "2025-10-01"
```

### 2.4 启动 Flink 流式处理

```bash
cd walmart_order
python flink_walmart_order_pipeline.py
```

**说明**：
- 从 Kafka topic `walmart_order_raw` 消费数据
- 解析订单 JSON 数据（一个订单可能包含多个订单行）
- 将解析后的数据写入 MySQL 表 `ods.walmart_order`
- 使用 Flink DataStream API，所有处理在同一个 Flink Job 内完成

**前置要求**：
- 确保 `flink_project/jar/` 目录包含以下 JAR 文件：
  - `flink-connector-kafka-3.1.0-1.17.jar`
  - `flink-connector-jdbc-3.1.1-1.17.jar`
  - `mysql-connector-java-8.0.28.jar`

### 2.5 验证数据

```sql
-- 查看 MySQL 中的数据
SELECT COUNT(*) FROM ods.walmart_order;
SELECT * FROM ods.walmart_order LIMIT 10;
```

## 3. 目录结构

```
stage1_basic_etl/
├── walmart_order/              # Walmart 订单处理模块
│   ├── request_order_and_push_kafka.py    # 获取订单并推送到 Kafka
│   ├── flink_walmart_order_pipeline.py    # Flink 流式处理管道
│   └── parse_walmart_order.py            # 订单解析工具
├── token_generator/             # 令牌生成器
│   └── walmart_access_token_generator.py
├── init_database/               # 数据库初始化
│   └── init_database_env.py
└── sql/                         # SQL 脚本
    └── create_walmart_order.sql
```

## 4. 配置文件

- **Kafka 配置**：`config/config.yml` 中的 `kafka` 部分
- **MySQL 配置**：`config/config.yml` 中的 `mysql` 部分
- **Walmart Token**：`config/config.yml` 中的 `walmart_token` 部分

## 5. 日志文件

所有脚本的日志文件保存在各模块的 `logs/` 目录下：
- `walmart_order/logs/request_order_and_push_kafka.log`
- `walmart_order/logs/flink_walmart_order_pipeline.log`

