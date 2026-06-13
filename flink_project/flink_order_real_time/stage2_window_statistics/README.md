# Stage2 Window Statistics

## 目标
- 基于 Kafka 订单流，进行窗口聚合与实时统计。
- 输出到 MySQL 统计表，供后续分析/展示使用。

## 计划功能
- 每分钟/每小时订单数与金额统计。
- 按用户的订单数/金额聚合。
- 按商品的销量/销售额聚合。
- 订单状态分布统计。

## 组件
- `order_statistics.py`：入口主程序，组装 Kafka Source、Watermark、窗口聚合、JDBC Sink。
- `window_aggregators.py`：聚合/Reduce函数、窗口计算逻辑。
- `real_time_dashboard.py`：可选的实时打印/可视化输出（开发阶段用于快速验证）。

## 依赖与环境
- Flink/PyFlink 1.17（与 Stage1 保持一致）。
- Kafka 连接器、JDBC 连接器同 Stage1（`flink_project/jar/1.17`）。
- Java 11。

## 下一步
1) 定义 Kafka Source schema（复用 stage1 的 topic `walmart_order_raw`）。  
2) 补充 Watermark 与事件时间字段。  
3) 实现窗口聚合（滚动窗口/滑动窗口）并写入 MySQL。  
4) 添加简单的控制台/日志输出以便验证聚合结果。  


