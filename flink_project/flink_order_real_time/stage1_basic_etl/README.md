# Stage1 Basic ETL

## 1. 项目概述

本项目实现 Walmart 订单数据的完整 ETL 流程：**API 获取 → Kafka → Flink 处理 → MySQL 存储**。

主要功能模块：
- **订单获取**：从 Walmart API 获取订单数据并推送到 Kafka
- **数据解析**：解析 Walmart 订单 JSON 数据，提取订单头和订单行信息
- **流式处理**：使用 Flink 从 Kafka 消费数据，解析后写入 MySQL
- **数据库初始化**：创建 ODS 层数据表
- **模块化设计**：采用模块化架构，支持 pymysql 和 Flink JDBC 两种数据库连接方式

数据流向：
```
Walmart API → Kafka (walmart_order_raw) → Flink → MySQL (ods.walmart_order)
```

## 1.1 架构特点

### 模块化设计
- **flink1_create_filnk_env**: 创建 Flink 执行环境
- **flink2_add_jar_to_flink**: 加载 Kafka/MySQL 所需 JAR 包
- **flink3_add_parameter_to_flink**: 配置并行度与 checkpoint
- **flink4_build_source**: 创建 Kafka Source 并生成 DataStream
- **flink5_process_and_sink**: 解析订单并写入 MySQL

### 数据库连接方式
**Flink JDBC 版本** (`flink6_walmart_order_pipeline.py`) - 使用 Flink 原生 JDBC 连接器（推荐）

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
python flink6_walmart_order_pipeline.py
```

**说明**：
- 从 Kafka topic `walmart_order_raw` 消费数据
- 解析订单 JSON 数据（一个订单可能包含多个订单行）
- 将解析后的数据写入 MySQL 表 `ods.walmart_order`
- 使用 Flink DataStream API，所有处理在同一个 Flink Job 内完成
- 支持本地模式运行，避免远程集群连接问题

**前置要求**：
- 确保 `flink_project/jar/` 目录包含以下 JAR 文件：
  - `flink-connector-kafka-3.1.0-1.17.jar`
  - `kafka-clients-3.4.1.jar`
  - `flink-connector-jdbc-3.1.1-1.17.jar` (JDBC 版本需要)
  - `mysql-connector-java-8.0.28.jar`

**JDBC 版本配置参数**：
```python
# 批处理配置
batch_size = 100              # 批处理大小
batch_interval_ms = 5000      # 批处理间隔（毫秒）
max_retries = 3               # 最大重试次数
```

### 2.5 验证数据

```sql
-- 查看 MySQL 中的数据
SELECT COUNT(*) FROM ods.walmart_order;
SELECT * FROM ods.walmart_order LIMIT 10;
```

## 3. 目录结构

```
stage1_basic_etl/
├── walmart_order/                          # Walmart 订单处理模块
│   ├── request_order_and_push_kafka.py    # 获取订单并推送到 Kafka
│   ├── flink6_walmart_order_pipeline.py   # Flink 流式处理管道 (JDBC 版本)
│   ├── flink1_create_filnk_env.py         # 模块1: 创建 Flink 环境
│   ├── flink2_add_jar_to_flink.py         # 模块2: 加载 JAR 包
│   ├── flink3_add_parameter_to_flink.py   # 模块3: 配置参数
│   ├── flink4_build_source.py             # 模块4: 构建数据源
│   ├── flink5_process_and_sink.py         # 模块5: 数据处理与输出 (pymysql)
│   ├── flink5_process_and_sink_jdbc.py    # 模块5: 数据处理与输出 (JDBC，包含 JDBC Sink 构建器)
│   ├── flink5_build_mysql_sink.py         # MySQL Sink 构建器 (pymysql)
│   ├── flink5_parse_walmart_order.py      # 订单解析工具
│   └── parse_walmart_order.py             # 订单解析工具 (兼容版本)
├── token_generator/                        # 令牌生成器
│   └── walmart_access_token_generator.py
├── init_database/                          # 数据库初始化
│   └── init_database_env.py
└── sql/                                    # SQL 脚本
    └── create_walmart_order.sql
```

## 4. 配置文件

- **Kafka 配置**：`config/config.yml` 中的 `kafka` 部分
- **MySQL 配置**：`config/config.yml` 中的 `mysql` 部分
- **Walmart Token**：`config/config.yml` 中的 `walmart_token` 部分

## 5. 日志文件

所有脚本的日志文件统一保存在项目根目录的 `logs/` 目录下：
- `logs/request_order_and_push_kafka.log`
- `logs/flink6_walmart_order_pipeline.log` (JDBC 版本)
- `logs/walmart_order_requester.log`

## 6. 性能优化建议

### 6.1 推荐使用 JDBC 版本
- **更高吞吐量**：支持批量插入，减少数据库连接开销
- **更好容错性**：内置重试机制和连接池管理
- **更强扩展性**：原生 Flink 性能，不受 Python GIL 限制

### 6.2 JDBC 版本调优参数
```python
# 在 flink6_walmart_order_pipeline.py 中调整
batch_size = 100              # 根据数据量调整批处理大小
batch_interval_ms = 5000      # 根据延迟要求调整批处理间隔
max_retries = 3               # 根据网络稳定性调整重试次数
parallelism = 1               # 根据资源情况调整并行度
```

### 6.3 环境要求
- **Python 环境**：推荐使用 conda 环境 py310
- **内存配置**：建议 JVM 堆内存至少 2GB
- **网络配置**：确保 Kafka 和 MySQL 网络连接稳定

## 7. 故障排除

### 7.1 常见问题
1. **JAR 包缺失**：确保所有必需的 JAR 包在 `flink_project/jar/` 目录下
2. **连接超时**：检查 Kafka 和 MySQL 服务状态及网络连接
3. **内存不足**：增加 JVM 堆内存或减少并行度
4. **数据格式错误**：检查 Kafka 中的 JSON 数据格式是否正确

### 7.2 调试建议
- 查看日志文件获取详细错误信息
- 使用本地模式避免远程集群连接问题
- 先测试小批量数据，确认流程正常后再处理大量数据

