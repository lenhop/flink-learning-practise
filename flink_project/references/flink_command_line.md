# Flink 命令行工具使用指南

## Docker 环境下的使用方式

在 Docker 环境下，Flink 命令需要在容器内执行。使用 `docker exec` 命令：

```bash
# 基本格式
docker exec flink-jobmanager-mysql ./bin/flink <command> [options]
```

## 常用管理命令

### 1. 作业列表

```bash
# 查看运行中的作业
docker exec flink-jobmanager-mysql ./bin/flink list

# 查看所有作业（包括已完成的）
docker exec flink-jobmanager-mysql ./bin/flink list -a

# 仅查看运行中的作业
docker exec flink-jobmanager-mysql ./bin/flink list -r

# 仅查看计划中的作业
docker exec flink-jobmanager-mysql ./bin/flink list -s
```

### 2. 提交作业

```bash
# 提交 Java/Scala 作业
docker exec flink-jobmanager-mysql ./bin/flink run \
  -c com.example.MainClass \
  /path/to/your-job.jar

# 提交 Python 作业
docker exec flink-jobmanager-mysql ./bin/flink run \
  --python /path/to/your_script.py

# 指定并行度
docker exec flink-jobmanager-mysql ./bin/flink run \
  -p 4 \
  -c com.example.MainClass \
  /path/to/your-job.jar

# 从 Savepoint 恢复
docker exec flink-jobmanager-mysql ./bin/flink run \
  -s /path/to/savepoint \
  -c com.example.MainClass \
  /path/to/your-job.jar
```

### 3. 停止作业

```bash
# 优雅停止（带 Savepoint）
docker exec flink-jobmanager-mysql ./bin/flink stop <job_id>

# 指定 Savepoint 路径
docker exec flink-jobmanager-mysql ./bin/flink stop \
  -p /path/to/savepoint \
  <job_id>
```

### 4. 取消作业

```bash
# 立即取消作业（不创建 Savepoint）
docker exec flink-jobmanager-mysql ./bin/flink cancel <job_id>
```

### 5. Savepoint 管理

```bash
# 触发 Savepoint
docker exec flink-jobmanager-mysql ./bin/flink savepoint \
  <job_id> \
  /path/to/savepoint

# 删除 Savepoint
docker exec flink-jobmanager-mysql ./bin/flink savepoint \
  -d /path/to/savepoint
```

### 6. Checkpoint 管理

```bash
# 手动触发 Checkpoint
docker exec flink-jobmanager-mysql ./bin/flink checkpoint <job_id>

# 触发完整 Checkpoint
docker exec flink-jobmanager-mysql ./bin/flink checkpoint -full <job_id>
```

### 7. 查看作业信息

```bash
# 查看作业执行计划（JSON 格式）
docker exec flink-jobmanager-mysql ./bin/flink info \
  -c com.example.MainClass \
  /path/to/your-job.jar
```

### 8. 查看帮助

```bash
# 查看所有命令帮助
docker exec flink-jobmanager-mysql ./bin/flink --help

# 查看特定命令帮助
docker exec flink-jobmanager-mysql ./bin/flink <command> --help
```

## 其他工具

### SQL 客户端

```bash
# 启动 SQL 客户端
docker exec -it flink-jobmanager-mysql ./bin/sql-client.sh

# 或使用嵌入式模式
docker exec -it flink-jobmanager-mysql ./bin/sql-client.sh embedded
```

### PyFlink Shell

```bash
# 启动 PyFlink Shell
docker exec -it flink-jobmanager-mysql ./bin/pyflink-shell.sh
```

## 注意事项

1. **容器名称**：根据实际容器名称替换 `flink-jobmanager-mysql`
2. **文件路径**：容器内的文件路径，如需访问本地文件需先复制到容器或使用 Docker 卷挂载
3. **Job ID**：可通过 `flink list` 命令获取
4. **Savepoint 路径**：需使用容器内可访问的路径（如 HDFS、S3 或挂载的卷）

## 快速参考

| 命令 | 用途 | 示例 |
|------|------|------|
| `flink list` | 列出作业 | `docker exec flink-jobmanager-mysql ./bin/flink list` |
| `flink run` | 提交作业 | `docker exec flink-jobmanager-mysql ./bin/flink run -c MainClass job.jar` |
| `flink stop` | 停止作业 | `docker exec flink-jobmanager-mysql ./bin/flink stop <job_id>` |
| `flink cancel` | 取消作业 | `docker exec flink-jobmanager-mysql ./bin/flink cancel <job_id>` |
| `flink savepoint` | 管理 Savepoint | `docker exec flink-jobmanager-mysql ./bin/flink savepoint <job_id> <path>` |
| `flink checkpoint` | 触发 Checkpoint | `docker exec flink-jobmanager-mysql ./bin/flink checkpoint <job_id>` |

