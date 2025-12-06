# Kafka 常用 Shell 命令

## Docker 环境使用说明

### 通过 Docker 执行命令

如果 Kafka 是通过 Docker 部署的，需要在主机上使用 `docker exec` 来执行容器内的命令：

```bash
# 方式 1: 在主机上执行，通过 docker exec 让容器执行命令（推荐）
docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:19092

# 方式 2: 进入容器后在容器内执行
docker exec -it broker-1 bash
# 以下命令在容器内执行
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:19092
exit
```

### 端口说明

根据 Docker Compose 配置：
- **从主机访问**: 使用 `localhost:29092` (broker-1) 或 `localhost:39092` (broker-2)
- **在容器内访问**: 
  - ✅ **必须使用 `19092` 端口**（容器间通信端口）
  - ✅ 使用 `localhost:19092`（当前容器内端口）
  - ✅ 使用服务名 `broker-1:19092`、`broker-2:19092`（推荐，适用于多 broker 集群）
  - ❌ **不能使用 `9092` 端口**（虽然容器内监听此端口，但会导致元数据返回错误的地址）

### 创建命令别名（可选）

**重要说明**: Kafka 命令工具（如 `kafka-topics.sh`）只存在于 Docker 容器内部，主机上**没有**这些文件。这是正常的，因为：
- Docker 容器有独立的文件系统
- `/opt/kafka/bin/` 路径只在容器内存在
- 主机上无法直接访问容器内的文件

为了方便使用，可以在 `~/.bashrc` 或 `~/.zshrc` 中添加别名：

```bash
# 在主机上添加 Kafka 命令别名（通过 Docker 执行容器内的命令）
alias kafka-topics='docker exec broker-1 /opt/kafka/bin/kafka-topics.sh'
alias kafka-console-producer='docker exec -it broker-1 /opt/kafka/bin/kafka-console-producer.sh'
alias kafka-console-consumer='docker exec -it broker-1 /opt/kafka/bin/kafka-console-consumer.sh'
alias kafka-consumer-groups='docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh'
alias kafka-configs='docker exec broker-1 /opt/kafka/bin/kafka-configs.sh'
alias kafka-get-offsets='docker exec broker-1 /opt/kafka/bin/kafka-get-offsets.sh'

# 使用示例（在主机上执行，添加别名后可以直接使用）
kafka-topics --list --bootstrap-server localhost:19092
```

**验证别名是否生效**:
```bash
# 在主机上重新加载 shell 配置
source ~/.bashrc  # 或 source ~/.zshrc

# 在主机上测试别名（通过 docker exec 让容器执行命令）
kafka-topics --list --bootstrap-server localhost:19092
```

**重要提示**: 以下命令示例中的 `localhost:9092` 是**通用示例**，实际使用时需要根据环境调整：

- **Docker 环境（在主机上执行，通过 docker exec 让容器执行命令）**:
  1. 在命令前加上 `docker exec broker-1` 或 `docker exec broker-2`
  2. 使用完整路径 `/opt/kafka/bin/` 前缀（该路径只在容器内存在）
  3. 将 `--bootstrap-server localhost:9092` 替换为：
     - `--bootstrap-server broker-1:19092`（推荐，使用服务名）
     - 或 `--bootstrap-server localhost:19092`（当前容器内端口）
  4. **重要**: 必须使用 `19092` 端口，不能使用 `9092` 端口（即使容器内监听 `9092`，也应使用 `19092` 进行容器间通信）

- **从主机访问 Docker 环境**:
  - 将 `--bootstrap-server localhost:9092` 替换为：
    - `--bootstrap-server localhost:29092` (broker-1)
    - 或 `--bootstrap-server localhost:39092` (broker-2)

- **非 Docker 环境（直接安装的 Kafka）**:
  - 使用 `localhost:9092` 或实际的 broker 地址

**常见问题**:

1. **文件不存在错误**:
   - ❌ `ls /opt/kafka/bin/kafka-topics.sh` → 文件不存在（主机上没有）
   - ✅ `docker exec broker-1 ls /opt/kafka/bin/kafka-topics.sh` → 文件存在（容器内有）

2. **连接错误：无法连接到 node 3/4 (localhost:29092/39092)**:

   错误信息示例：
   ```
   WARN [AdminClient clientId=adminclient-1] Connection to node 4 (localhost/127.0.0.1:39092) 
   could not be established. Node may not be available.
   ```

   **原因**: 在容器内使用了错误的端口号。`localhost:9092`、`localhost:29092`、`localhost:39092` 都是主机端口映射，在容器内无法访问。当 Kafka 客户端尝试连接集群时，会从元数据中获取其他 broker 的 advertised.listeners（如 `PLAINTEXT_HOST://localhost:29092`），但在容器内 `localhost` 指向容器本身，无法访问其他 broker 容器。

   **解决方案**: 在容器内必须使用容器间通信地址和 `19092` 端口：
   - 使用服务名和容器间通信端口：`broker-1:19092`（推荐，适用于多 broker 集群）
   - 或使用当前容器内端口：`localhost:19092`（仅限单 broker 场景）
   - **关键**: 必须使用 `19092` 端口，不能使用 `9092` 端口

   ```bash
   # ❌ 错误：使用主机端口（在容器内无法访问）
   docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
     --bootstrap-server localhost:29092

   # ❌ 错误：使用容器内 9092 端口（会导致元数据返回错误地址）
   docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
     --bootstrap-server localhost:9092

   # ❌ 错误：使用服务名但端口错误
   docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
     --bootstrap-server broker-1:9092

   # ✅ 正确：使用容器内 19092 端口
   docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
     --bootstrap-server localhost:19092

   # ✅ 正确：使用服务名和容器间通信端口（推荐，适用于多 broker 集群）
   docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
     --bootstrap-server broker-1:19092
   ```

## Topic 管理

### 创建 Topic

```bash
# 通用示例（适用于非 Docker 环境或在容器内执行，实际使用时根据环境调整端口）
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 1

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server broker-1:19092 \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 1

# 创建时指定配置（通用示例，适用于非 Docker 环境或在容器内执行）
# 各参数说明:
# --bootstrap-server       Kafka 集群 broker 地址（必需），多个用逗号分隔
# --topic                 topic 名称
# --partitions            分区数
# --replication-factor    副本因子（副本个数）
# --config                topic 配置项，可以设置多个，格式为 key=value
#     retention.ms           消息保留时间（毫秒，例：86400000 = 1 天）, Kafka 的默认保留策略 是 7天
#     max.message.bytes      每条消息最大字节数（例：1048576 = 1MB）
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \         # Kafka broker 地址
  --topic my-topic \                          # topic 名称
  --partitions 3 \                            # 分区数
  --replication-factor 1 \                    # 副本因子
  --config retention.ms=86400000 \            # 消息保留 1 天
  --config max.message.bytes=1048576          # 每条消息最大 1MB
```

### 删除 Topic

```bash
# 删除单个 topic（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-topics.sh --delete \
  --bootstrap-server localhost:9092 \
  --topic my-topic

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --delete \
#   --bootstrap-server broker-1:19092 \
#   --topic my-topic

# 删除多个 topic（需要先修改 server.properties 启用删除功能）
# delete.topic.enable=true
```

### 列出所有 Topic

```bash
# 列出所有 topic（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-topics.sh --list \
  --bootstrap-server localhost:9092

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
#   --bootstrap-server broker-1:19092

# 列出包含指定字符串的 topic（通用示例）
kafka-topics.sh --list \
  --bootstrap-server localhost:9092 \
  | grep "my-prefix"
```

### 检查 Topic 是否存在

```bash
# 检查 topic 是否存在（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-topics.sh --list \
  --bootstrap-server localhost:9092 \
  | grep -q "^my-topic$" && echo "存在" || echo "不存在"

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --list \
#   --bootstrap-server broker-1:19092 \
#   | grep -q "^my-topic$" && echo "存在" || echo "不存在"
```

## Topic 查询

### 描述 Topic 详情

```bash
# 查看 topic 详细信息（分区、副本、Leader 等）（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic my-topic

# 查看所有 topic 的详细信息（通用示例）
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092

# 仅显示分区信息（通用示例）
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  | grep "Partition:"

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令，仅显示分区信息）
docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server broker-1:19092 \
  --topic my-topic \
  | grep "Partition:"
```

### 查看 Topic 消息数

```bash
# 查看 topic 各分区的消息数（通用示例，适用于非 Docker 环境或在容器内执行）
# 最新版本使用 kafka-get-offsets.sh
kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --time -1

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-get-offsets.sh \
#   --bootstrap-server broker-1:19092 \
#   --topic my-topic \
#   --time -1

# 统计总消息数（通用示例）
kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --time -1 \
  | awk -F: '{sum+=$3} END {print sum}'
```

### 查看 Topic 配置

```bash
# 查看 topic 所有配置（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-configs.sh --describe \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-configs.sh --describe \
#   --bootstrap-server broker-1:19092 \
#   --entity-type topics \
#   --entity-name my-topic

# 查看 broker 默认配置（通用示例）
kafka-configs.sh --describe \
  --bootstrap-server localhost:9092 \
  --entity-type brokers \
  --entity-default
```

## Topic 配置

### 修改 Topic 配置

```bash
# 修改单个配置项（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-configs.sh --alter \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --add-config retention.ms=86400000

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-configs.sh --alter \
#   --bootstrap-server broker-1:19092 \
#   --entity-type topics \
#   --entity-name my-topic \
#   --add-config retention.ms=86400000

# 修改多个配置项（通用示例）
kafka-configs.sh --alter \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --add-config retention.ms=86400000,max.message.bytes=1048576

# 删除配置项（恢复默认值）（通用示例）
kafka-configs.sh --alter \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --delete-config retention.ms
```

### 常用配置项

```bash
# 消息保留时间（毫秒）
--add-config retention.ms=86400000

# 最大消息大小（字节）
--add-config max.message.bytes=1048576

# 压缩类型
--add-config compression.type=gzip

# 清理策略
--add-config cleanup.policy=delete
```

## Topic 修正

### 增加分区数

```bash
# 增加 topic 分区数（只能增加，不能减少）（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-topics.sh --alter \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 6

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --alter \
#   --bootstrap-server broker-1:19092 \
#   --topic my-topic \
#   --partitions 6
```

### 修改副本数

```bash
# 修改 topic 副本数（需要先创建分区分配计划）（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassign.json \
  --execute

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
#   --bootstrap-server broker-1:19092 \
#   --reassignment-json-file reassign.json \
#   --execute
```

## Consumer Group 管理

### 列出 Consumer Group

```bash
# 列出所有 consumer group（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
#   --bootstrap-server broker-1:19092 --list

# 查看 consumer group 详情（通用示例）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --describe
```

### 查看 Consumer Lag

```bash
# 查看 consumer group 的 lag（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --describe

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
#   --bootstrap-server broker-1:19092 \
#   --group my-group \
#   --describe

# 仅显示 lag 信息（通用示例）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --describe \
  | awk '{print $1, $2, $5, $6}'
```

### 重置 Consumer Offset

```bash
# 重置到最早位置（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --topic my-topic \
  --reset-offsets \
  --to-earliest \
  --execute

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
#   --bootstrap-server broker-1:19092 \
#   --group my-group \
#   --topic my-topic \
#   --reset-offsets \
#   --to-earliest \
#   --execute

# 重置到最新位置（通用示例）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --topic my-topic \
  --reset-offsets \
  --to-latest \
  --execute

# 重置到指定 offset（通用示例）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --topic my-topic:0 \
  --reset-offsets \
  --to-offset 100 \
  --execute
```

### 删除 Consumer Group

```bash
# 删除 consumer group（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --delete

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
#   --bootstrap-server broker-1:19092 \
#   --group my-group \
#   --delete
```

## 消息操作

### 生产消息

```bash
# 交互式生产消息（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic my-topic

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec -it broker-1 /opt/kafka/bin/kafka-console-producer.sh \
#   --bootstrap-server broker-1:19092 \
#   --topic my-topic

# 从文件读取消息（通用示例）
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic my-topic < messages.txt

# 指定 key（通用示例）
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic my-topic \
  --property "parse.key=true" \
  --property "key.separator=:"
```

### 消费消息

```bash
# 从最早位置消费（从 topic 开始位置）（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec -it broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
#   --bootstrap-server broker-1:19092 \
#   --topic my-topic \
#   --from-beginning

# 从最新位置消费（默认，只消费新消息）（通用示例）
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic my-topic

# 指定 consumer group（通用示例）
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic my-topic \
  --group my-group

# 显示 key 和 value（通用示例）
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic my-topic \
  --property print.key=true \
  --property print.value=true \
  --from-beginning

# 限制消费数量（通用示例）
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic my-topic \
  --max-messages 10 \
  --from-beginning
```

## 集群管理

### 查看 Broker 信息

```bash
# 查看集群 broker 信息（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
#   --bootstrap-server broker-1:19092
```

### 查看集群元数据

```bash
# 查看集群元数据（需要先导出 snapshot 文件）（通用示例，适用于非 Docker 环境或在容器内执行）
# 注意：kafka-metadata-shell.sh 需要 snapshot 文件，不支持直接连接 bootstrap-server
# 使用方式：
# 1. 先导出 metadata snapshot（需要访问 Kafka 数据目录）
# 2. 使用 kafka-metadata-shell.sh --snapshot <snapshot-file> 查看
kafka-metadata-shell.sh --snapshot /path/to/snapshot-file

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-metadata-shell.sh \
#   --snapshot /path/to/snapshot-file
```

## 性能测试

### Producer 性能测试

```bash
# 测试 producer 性能（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-producer-perf-test.sh --topic my-topic \
  --num-records 100000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
#   --topic my-topic \
#   --num-records 100000 \
#   --record-size 1024 \
#   --throughput 1000 \
#   --producer-props bootstrap.servers=broker-1:19092
```

### Consumer 性能测试

```bash
# 测试 consumer 性能（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-consumer-perf-test.sh --topic my-topic \
  --messages 100000 \
  --bootstrap-server localhost:9092

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
#   --topic my-topic \
#   --messages 100000 \
#   --bootstrap-server broker-1:19092
```

## 实用技巧

### 批量操作

```bash
# 批量创建 topic（通过脚本）（通用示例，适用于非 Docker 环境或在容器内执行）
for topic in topic1 topic2 topic3; do
  kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic $topic \
    --partitions 3 \
    --replication-factor 1
done

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# for topic in topic1 topic2 topic3; do
#   docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --create \
#     --bootstrap-server broker-1:19092 \
#     --topic $topic \
#     --partitions 3 \
#     --replication-factor 1
# done

# 批量删除 topic（通用示例）
for topic in topic1 topic2 topic3; do
  kafka-topics.sh --delete \
    --bootstrap-server localhost:9092 \
    --topic $topic
done
```

### 导出导入配置

```bash
# 导出 topic 配置到文件（通用示例，适用于非 Docker 环境或在容器内执行）
kafka-configs.sh --describe \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic > topic-config.txt

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# docker exec broker-1 /opt/kafka/bin/kafka-configs.sh --describe \
#   --bootstrap-server broker-1:19092 \
#   --entity-type topics \
#   --entity-name my-topic > topic-config.txt

# 从文件读取配置并应用
# （需要手动解析文件并转换为 --add-config 参数）
```

### 监控命令

```bash
# 实时监控 topic 消息数（通用示例，适用于非 Docker 环境或在容器内执行）
watch -n 1 'kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --time -1'

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# watch -n 1 'docker exec broker-1 /opt/kafka/bin/kafka-get-offsets.sh \
#   --bootstrap-server broker-1:19092 \
#   --topic my-topic \
#   --time -1'

# 监控 consumer lag（通用示例）
watch -n 1 'kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group \
  --describe'

# Docker 环境示例（在主机上执行，通过 docker exec 让容器执行命令）
# watch -n 1 'docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
#   --bootstrap-server broker-1:19092 \
#   --group my-group \
#   --describe'
```

## 注意事项

1. **删除 Topic**: 需要确保 `server.properties` 中 `delete.topic.enable=true`
2. **分区数**: 只能增加分区数，不能减少
3. **副本数**: 修改副本数需要重新分配分区，操作较复杂
4. **配置修改**: 某些配置修改后需要重启 broker 才能生效
5. **性能测试**: 生产环境谨慎使用性能测试工具，避免影响正常业务
6. **Docker 环境端口**: 
   - 在主机上通过 `docker exec` 执行容器内命令时，必须使用容器间通信地址和 `19092` 端口（`broker-X:19092` 或 `localhost:19092`）
   - **不能使用 `9092` 端口**，即使容器内监听此端口，也会导致 Kafka 元数据返回错误的地址，无法访问其他 broker
   - 从主机访问时，使用主机端口映射（`localhost:29092/39092`）
   - 使用 `localhost:9092` 或 `broker-1:9092` 在容器内会失败，因为 Kafka 元数据返回的地址在容器内无法访问
