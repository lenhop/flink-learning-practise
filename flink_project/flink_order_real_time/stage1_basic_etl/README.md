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

#### 订单获取模块
- **order1_request_walmart_order.py**: Walmart 订单请求服务（含 token 获取与自动拆分）
- **order2_push_order_to_kafka.py**: Kafka 推送服务（topic 确认与消息发送）
- **order3_inte_request_and_push.py**: 集成 CLI 工具（整合订单请求和 Kafka 推送）

#### Flink 处理模块
- **flink1_create_filnk_env.py**: 创建 Flink 执行环境
- **flink2_add_jar_to_flink.py**: 加载 Kafka/MySQL 所需 JAR 包
- **flink3_add_parameter_to_flink.py**: 配置并行度与 checkpoint
- **flink4_build_source.py**: 创建 Kafka Source 并生成 DataStream
- **flink5_process_and_sink_jdbc.py**: 解析订单并使用 Flink JDBC 写入 MySQL（推荐）
- **flink5_build_mysql_sink.py**: MySQL Sink 构建器（pymysql 版本，已弃用）
- **flink6_walmart_order_pipeline.py**: 主流程管道（整合所有 Flink 模块）

### 数据库连接方式
**Flink JDBC 版本** (`flink6_walmart_order_pipeline.py`) - 使用 Flink 原生 JDBC 连接器（推荐）
- 支持批量插入，性能更优
- 内置重试机制和连接池管理
- 原生 Flink 性能，不受 Python GIL 限制

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

使用集成 CLI 工具 `order3_inte_request_and_push.py`：

```bash
cd walmart_order

# 方式1: 获取最近 N 小时的数据（PST 时区）
python order3_inte_request_and_push.py -H 2

# 方式2: 获取指定日期范围的数据
python order3_inte_request_and_push.py -s 20251111 -e 20251111

# 方式3: 指定账户、topic 和 ship node 类型
python order3_inte_request_and_push.py -H 2 -a eForCity -t walmart_order_raw --ship-node-type SellerFulfilled
```

**CLI 参数说明**：
- `-H, --hours N`: 获取最近 N 小时的数据（PST 时区）
- `-s, --start-date YYYYMMDD`: 开始日期（需配合 `-e` 使用）
- `-e, --end-date YYYYMMDD`: 结束日期（需配合 `-s` 使用）
- `-a, --account-tag TAG`: Walmart 账户标签（默认：eForCity）
- `-t, --topic TOPIC`: Kafka topic 名称（默认：walmart_order_raw）
- `--ship-node-type TYPE`: Ship node 类型（可选：SellerFulfilled, WFSFulfilled, 3PLFulfilled）
- `--batch-size N`: Kafka 推送批次大小（默认：20）

**功能说明**：
- 自动获取 Walmart API access token
- 支持时间范围自动拆分（按小时或 10 分钟）
- 自动创建 Kafka topic（如果不存在）
- 批量推送订单数据到 Kafka
- 支持 token 过期自动刷新重试

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

**JDBC 版本配置参数**（在 `flink6_walmart_order_pipeline.py` 中）：
```python
# 批处理配置（在 build_pipeline 函数中）
batch_size = 10                # 批处理大小（推荐较小值以提高可靠性）
batch_interval_ms = 1000       # 批处理间隔（毫秒）
max_retries = 3                # 最大重试次数

# 并行度配置（在 build_env_and_jars 函数中）
parallelism = 1                # Flink 并行度

# Checkpoint 配置
checkpoint_interval_ms = 10000 # Checkpoint 间隔（毫秒）
```

**运行说明**：
- 使用 `python -u` 运行以确保日志实时输出
- 程序会持续运行，等待 Kafka 数据并自动处理
- 按 `Ctrl+C` 可停止程序

### 2.5 验证数据

```sql
-- 查看 MySQL 中的数据
SELECT COUNT(*) FROM ods.walmart_order;
SELECT * FROM ods.walmart_order LIMIT 10;
```

## 3. 环境要求

- Flink 集群：**Flink 1.17**（推荐使用 Docker 部署，如官方镜像 `flink:1.17-scala_2.12`）
- PyFlink 版本：**1.17**（需与 Flink 版本一致）
- JAR 依赖（位于 `flink_project/jar/1.17/`，均为 1.17 适配版）：
  - `flink-core-1.17.1.jar`
  - `flink-connector-jdbc-3.1.1-1.17.jar`
  - `flink-connector-kafka-3.1.0-1.17.jar`
  - `flink-connector-kafka-1.17.1.jar`
- Java 版本：**Java 11**

## 4. 目录结构

```
stage1_basic_etl/
├── walmart_order/                          # Walmart 订单处理模块
│   ├── order1_request_walmart_order.py    # 订单请求服务（含 token 获取与自动拆分）
│   ├── order2_push_order_to_kafka.py      # Kafka 推送服务
│   ├── order3_inte_request_and_push.py    # 集成 CLI 工具（订单请求 + Kafka 推送）
│   ├── flink6_walmart_order_pipeline.py   # Flink 流式处理管道主程序（JDBC 版本）
│   ├── flink1_create_filnk_env.py         # Flink 模块1: 创建执行环境
│   ├── flink2_add_jar_to_flink.py         # Flink 模块2: 加载 JAR 包
│   ├── flink3_add_parameter_to_flink.py   # Flink 模块3: 配置参数（并行度、checkpoint）
│   ├── flink4_build_source.py             # Flink 模块4: 构建 Kafka Source
│   ├── flink5_process_and_sink_jdbc.py    # Flink 模块5: 数据处理与 JDBC Sink（推荐）
│   ├── flink5_build_mysql_sink.py         # MySQL Sink 构建器（pymysql 版本，已弃用）
│   └── flink5_parse_walmart_order.py       # 订单解析工具（JSON 转 tuple）
├── token_generator/                        # 令牌生成器
│   └── walmart_access_token_generator.py
├── init_database/                          # 数据库初始化
│   └── init_database_env.py
└── sql/                                    # SQL 脚本
    └── create_walmart_order.sql
```

## 5. 配置文件

- **Kafka 配置**：`config/config.yml` 中的 `kafka` 部分
- **MySQL 配置**：`config/config.yml` 中的 `mysql` 部分
- **Walmart Token**：`config/config.yml` 中的 `walmart_token` 部分

## 6. 日志文件

所有脚本的日志文件统一保存在项目根目录的 `logs/` 目录下：
- `logs/order1_request_walmart_order.log` - 订单请求日志
- `logs/order2_push_order_to_kafka.log` - Kafka 推送日志
- `logs/order3_inte_request_and_push.log` - 集成工具日志
- `logs/flink6_walmart_order_pipeline.log` - Flink 处理管道日志（JDBC 版本）

**日志特点**：
- 所有日志同时输出到文件和控制台
- Flink 管道日志支持实时刷新显示（使用 `python -u` 运行）
- 日志格式：`YYYY-MM-DD HH:MM:SS,mmm - LEVEL - MESSAGE`

## 7. 性能优化建议

### 7.1 推荐使用 JDBC 版本
- **更高吞吐量**：支持批量插入，减少数据库连接开销
- **更好容错性**：内置重试机制和连接池管理
- **更强扩展性**：原生 Flink 性能，不受 Python GIL 限制

### 7.2 JDBC 版本调优参数
```python
# 在 flink6_walmart_order_pipeline.py 的 build_pipeline 函数中调整
batch_size = 10               # 批处理大小（推荐较小值，如 10-50，提高可靠性）
batch_interval_ms = 1000      # 批处理间隔（毫秒，推荐 1000-5000）
max_retries = 3               # 最大重试次数

# 在 build_env_and_jars 函数中调整
parallelism = 1               # Flink 并行度（根据资源情况调整）
checkpoint_interval_ms = 10000  # Checkpoint 间隔（毫秒，推荐 10000-60000）
```

**性能调优建议**：
- **小批量数据**：`batch_size=10`, `batch_interval_ms=1000`（当前配置）
- **大批量数据**：`batch_size=100`, `batch_interval_ms=5000`
- **低延迟要求**：减小 `batch_interval_ms`，但会增加数据库连接压力
- **高吞吐量要求**：增大 `batch_size` 和 `parallelism`

### 7.3 环境要求
- **Python 环境**：推荐使用 conda 环境 py310
- **内存配置**：建议 JVM 堆内存至少 2GB
- **网络配置**：确保 Kafka 和 MySQL 网络连接稳定

## 8. 故障排除

### 8.1 常见问题
1. **JAR 包缺失**：确保所有必需的 JAR 包在 `flink_project/jar/` 目录下
2. **连接超时**：检查 Kafka 和 MySQL 服务状态及网络连接
3. **内存不足**：增加 JVM 堆内存或减少并行度
4. **数据格式错误**：检查 Kafka 中的 JSON 数据格式是否正确

### 8.2 调试建议
- 查看日志文件获取详细错误信息
- 使用本地模式（`force_local_mode=True`）避免远程集群连接问题
- 先测试小批量数据，确认流程正常后再处理大量数据
- 使用 `python -u` 运行 Flink 管道以确保日志实时输出
- 检查 Kafka consumer group offset 确认数据是否被消费
- 验证 MySQL 表结构和字段类型是否匹配

### 8.3 常用调试命令

```bash
# 检查 Kafka topic 和 consumer group
docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 \
  --describe --group flink-walmart-order-pipeline-jdbc

# 检查 MySQL 数据
mysql -u root -p -e "SELECT COUNT(*) FROM ods.walmart_order;"

# 查看 Flink 日志
tail -f logs/flink6_walmart_order_pipeline.log
```

