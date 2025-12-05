# Flink 电商订单实时 ETL 学习项目计划

## 项目概述

**项目名称**: Flink Order Real-time ETL Learning Project  
**项目位置**: `/Users/hzz/KMS/flink-learning-practise/flink_project/flink_order_real_time`  
**技术栈**: Flink 1.20.3 + Python 3.11 (PyFlink) + Kafka + MySQL  
**架构流程**: Order API → Kafka → Flink → MySQL

## 项目目标

1. 学习 Flink 核心功能和 API
2. 掌握实时 ETL 数据处理流程
3. 实现电商订单数据的实时处理、转换、统计分析
4. 通过渐进式学习，从基础到高级逐步深入

## 阶段划分

### 阶段1: 基础 ETL - 数据流转 (Week 1)

**学习目标**:
- Flink 基础概念和环境搭建
- Kafka Source 和 JDBC Sink 的使用
- 简单的数据流转

**实现功能**:
1. 从 Kafka 读取订单原始数据
2. 解析 JSON 数据
3. 写入 MySQL 订单表

**Flink 功能点**:
- StreamExecutionEnvironment
- KafkaSource
- JdbcSink
- 基本的数据类型转换

**文件结构**:
```
flink_order_real_time/
├── stage1_basic_etl/
│   ├── order_producer.py          # 模拟订单 API，发送数据到 Kafka
│   ├── flink_order_etl.py         # Flink ETL 主程序
│   └── README.md                  # 阶段说明文档
├── sql/
│   └── create_tables.sql          # 创建数据库表结构
└── config/
    └── config.yml                 # 配置文件
```

**预期输出**:
- 订单数据成功从 Kafka 流转到 MySQL
- 理解 Flink 的基本数据流处理

---

### 阶段2: 数据转换和清洗 (Week 2)

**学习目标**:
- Flink 数据转换操作（map, filter, flatMap）
- 数据清洗和验证
- 错误数据处理

**实现功能**:
1. 数据清洗（去除无效数据、格式转换）
2. 数据验证（订单金额校验、状态校验）
3. 数据拆分（订单主表和明细表分离）
4. 错误数据记录到日志或死信队列

**Flink 功能点**:
- MapFunction / FlatMapFunction
- FilterFunction
- 自定义函数（UDF）
- 侧输出流（Side Output）处理错误数据

**文件结构**:
```
stage2_data_transformation/
├── order_transformer.py            # 数据转换主程序
├── order_validator.py              # 数据验证函数
├── order_splitter.py               # 订单拆分逻辑
└── README.md
```

**预期输出**:
- 清洗后的订单数据写入 MySQL
- 错误数据单独处理
- 订单主表和明细表正确分离

---

### 阶段3: 窗口操作和实时统计 (Week 3)

**学习目标**:
- Flink 窗口操作（时间窗口、计数窗口）
- 实时聚合计算
- Watermark 和事件时间处理

**实现功能**:
1. 实时订单统计（每分钟/每小时订单数、金额）
2. 用户订单统计（每个用户的订单数、总金额）
3. 商品销售统计（每个商品的销量、销售额）
4. 订单状态变化监控

**Flink 功能点**:
- Window 操作（TumblingWindow, SlidingWindow, SessionWindow）
- AggregateFunction / ReduceFunction
- Watermark 策略
- 事件时间 vs 处理时间
- KeyedStream 操作

**文件结构**:
```
stage3_window_statistics/
├── order_statistics.py             # 实时统计主程序
├── window_aggregators.py           # 窗口聚合函数
├── real_time_dashboard.py          # 实时统计结果输出
└── README.md
```

**统计指标**:
- 每分钟订单数、订单金额
- 每小时订单数、订单金额
- 每个用户的订单统计
- 每个商品的销售统计
- 订单状态分布

**预期输出**:
- 实时统计结果写入 MySQL 统计表
- 理解窗口操作和聚合计算
- 掌握事件时间处理

---

### 阶段4: 状态管理和复杂事件处理 (Week 4)

**学习目标**:
- Flink 状态管理（KeyedState, OperatorState）
- 复杂事件处理（CEP）
- 状态后端配置
- Checkpoint 和容错

**实现功能**:
1. 订单状态机（状态转换跟踪）
2. 异常订单检测（大额订单、频繁下单）
3. 订单关联分析（同一用户连续订单）
4. 订单超时检测（未支付订单超时）

**Flink 功能点**:
- KeyedState (ValueState, ListState, MapState)
- ProcessFunction（底层处理函数）
- CEP（Complex Event Processing）
- Checkpoint 配置
- 状态后端（RocksDB）

**文件结构**:
```
stage4_state_cep/
├── order_state_machine.py          # 订单状态机
├── order_anomaly_detector.py       # 异常订单检测
├── order_cep_processor.py          # CEP 复杂事件处理
├── order_timeout_detector.py       # 订单超时检测
└── README.md
```

**复杂场景**:
- 订单状态转换跟踪
- 大额订单预警（单笔订单 > 10000）
- 频繁下单检测（1分钟内 > 5 单）
- 未支付订单超时（30分钟未支付）
- 用户购买行为分析

**预期输出**:
- 实现状态管理和复杂事件处理
- 理解 Flink 容错机制
- 掌握高级流处理模式

---

## 技术架构

### 系统架构图

```
┌─────────────┐
│ Order API   │ (模拟订单生成)
└──────┬──────┘
       │ JSON
       ▼
┌─────────────┐
│   Kafka     │ (order-raw topic)
└──────┬──────┘
       │ Consumer
       ▼
┌─────────────┐
│   Flink     │ (数据处理)
│  JobManager │
│ TaskManager │
└──────┬──────┘
       │ Processed Data
       ▼
┌─────────────┐
│   MySQL     │ (订单表、统计表)
└─────────────┘
```

### 技术组件

**Flink 集群**:
- JobManager: 1 个
- TaskManager: 1-2 个
- 使用 Docker Compose 部署

**Kafka 集群**:
- 复用现有的 Kafka 集群
- Topic: order-raw, order-processed, order-statistics

**MySQL 数据库**:
- 订单表（order_header, order_line）
- 统计表（order_statistics_minute, order_statistics_hour）
- 异常记录表（order_anomalies）

## 项目文件结构

```
flink_order_real_time/
├── README.md                       # 项目总说明
├── flink_order_real_time_plan.md  # 本计划文档
├── docker-compose.yml              # Flink 集群配置
├── requirements.txt                # Python 依赖
├── config/
│   ├── config.yml                  # 配置文件
│   └── flink_config.yml            # Flink 配置
├── sql/
│   └── create_tables.sql           # 数据库表结构
├── stage1_basic_etl/               # 阶段1：基础 ETL
│   ├── order_producer.py
│   ├── flink_order_etl.py
│   └── README.md
├── stage2_data_transformation/     # 阶段2：数据转换
│   ├── order_transformer.py
│   ├── order_validator.py
│   ├── order_splitter.py
│   └── README.md
├── stage3_window_statistics/        # 阶段3：窗口统计
│   ├── order_statistics.py
│   ├── window_aggregators.py
│   ├── real_time_dashboard.py
│   └── README.md
├── stage4_state_cep/                # 阶段4：状态和 CEP
│   ├── order_state_machine.py
│   ├── order_anomaly_detector.py
│   ├── order_cep_processor.py
│   ├── order_timeout_detector.py
│   └── README.md
├── utils/
│   ├── kafka_utils.py              # Kafka 工具函数
│   ├── mysql_utils.py              # MySQL 工具函数
│   └── order_models.py             # 订单数据模型
└── tests/
    ├── test_order_producer.py      # 测试订单生成
    └── test_flink_jobs.py          # 测试 Flink 作业
```

## 学习路径

### Week 1: 基础入门
- Day 1-2: 环境搭建和基础概念
- Day 3-4: 实现阶段1基础 ETL
- Day 5: 测试和文档整理

### Week 2: 数据转换
- Day 1-2: 学习 Flink 转换操作
- Day 3-4: 实现阶段2数据清洗
- Day 5: 优化和测试

### Week 3: 窗口和统计
- Day 1-2: 学习窗口操作和 Watermark
- Day 3-4: 实现阶段3实时统计
- Day 5: 性能优化

### Week 4: 高级功能
- Day 1-2: 学习状态管理和 CEP
- Day 3-4: 实现阶段4复杂处理
- Day 5: 项目总结和文档

## 预期成果

1. **代码成果**:
   - 4 个阶段的完整代码实现
   - 可运行的 Flink 作业
   - 完整的测试用例

2. **知识成果**:
   - 掌握 Flink 核心 API
   - 理解实时流处理模式
   - 熟悉电商数据处理场景

3. **文档成果**:
   - 每个阶段的 README 文档
   - 代码注释和说明
   - 学习笔记和总结

## 下一步行动

1. 创建项目目录结构
2. 初始化数据库表结构
3. 实现阶段1基础 ETL
4. 逐步完成后续阶段

## 参考资料

- [Apache Flink 官方文档](https://flink.apache.org/docs/)
- [PyFlink 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/overview/)
- [Flink 最佳实践](https://flink.apache.org/docs/stable/best_practices/)

