#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flink 开发环境检测脚本
检查 Java、Python、PyFlink 及相关依赖是否满足开发要求
"""

import sys
import subprocess
import importlib
from typing import List, Tuple


def print_section(title: str):
    """Print section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_item(label: str, status: str, details: str = ""):
    """Print check item"""
    status_symbol = "✓" if status == "OK" else "✗" if status == "FAIL" else "⚠"
    print(f"{status_symbol} {label:40s} {status}")
    if details:
        print(f"  {details}")


def check_java() -> Tuple[str, str]:
    """Check Java version"""
    try:
        # Java outputs version to stderr
        result = subprocess.run(
            ["java", "-version"],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            timeout=5
        )
        version_info = result.stderr.strip() if result.stderr else result.stdout.strip()
        
        # Extract version number
        version_line = version_info.split('\n')[0] if version_info else ""
        if "17" in version_line or "openjdk version \"17" in version_info:
            return "OK", version_line
        elif "11" in version_line or "openjdk version \"11" in version_info:
            return "OK", version_line + " (推荐使用 Java 17)"
        elif "8" in version_line or "1.8" in version_info:
            return "WARN", version_line + " (建议升级到 Java 11 或 17)"
        else:
            return "WARN", version_line + " (Flink 推荐 Java 8/11/17)"
    except Exception as e:
        return "FAIL", f"无法检测 Java: {e}"


def check_python() -> Tuple[str, str]:
    """Check Python version"""
    version = sys.version_info
    version_str = f"{version.major}.{version.minor}.{version.micro}"
    
    if version.major == 3 and version.minor >= 8:
        return "OK", f"Python {version_str} (路径: {sys.executable})"
    else:
        return "FAIL", f"Python {version_str} (需要 Python 3.8+)"


def check_pyflink_modules() -> List[Tuple[str, str, str]]:
    """Check PyFlink modules"""
    results = []
    
    modules_to_check = [
        ("pyflink", "PyFlink 核心模块"),
        ("pyflink.datastream", "DataStream API"),
        ("pyflink.table", "Table API"),
        ("pyflink.common", "通用模块"),
        ("pyflink.datastream.connectors.kafka", "Kafka 连接器"),
        ("pyflink.datastream.connectors.jdbc", "JDBC 连接器"),
    ]
    
    for module_name, description in modules_to_check:
        try:
            module = importlib.import_module(module_name)
            version = getattr(module, '__version__', '未知版本')
            results.append(("OK", f"{description} ({module_name})", f"版本: {version}"))
        except ImportError as e:
            results.append(("FAIL", f"{description} ({module_name})", f"导入失败: {e}"))
    
    return results


def check_pyflink_classes() -> List[Tuple[str, str, str]]:
    """Check PyFlink key classes"""
    results = []
    
    classes_to_check = [
        ("pyflink.datastream", "StreamExecutionEnvironment", "DataStream 执行环境"),
        ("pyflink.table", "TableEnvironment", "Table API 环境"),
        ("pyflink.datastream.connectors.kafka", "KafkaSource", "Kafka 数据源"),
        ("pyflink.datastream.connectors.kafka", "KafkaSink", "Kafka 数据汇"),
        ("pyflink.datastream.connectors.jdbc", "JdbcSink", "JDBC 数据汇"),
        ("pyflink.common.watermark_strategy", "WatermarkStrategy", "水印策略"),
    ]
    
    for module_name, class_name, description in classes_to_check:
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            results.append(("OK", f"{description}", f"{module_name}.{class_name}"))
        except (ImportError, AttributeError) as e:
            results.append(("FAIL", f"{description}", f"失败: {e}"))
    
    return results


def check_pyflink_runtime() -> Tuple[str, str]:
    """Check if PyFlink can create execution environments"""
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import TableEnvironment, EnvironmentSettings
        
        env = StreamExecutionEnvironment.get_execution_environment()
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        
        return "OK", "可以创建执行环境"
    except Exception as e:
        return "FAIL", f"无法创建执行环境: {e}"


def check_other_dependencies() -> List[Tuple[str, str, str]]:
    """Check other Python dependencies"""
    results = []
    
    # Check using pip list instead of import to avoid import errors
    try:
        pip_result = subprocess.run(
            ["pip", "list"],
            capture_output=True,
            text=True,
            timeout=10
        )
        installed_packages = pip_result.stdout.lower()
    except Exception:
        installed_packages = ""
    
    dependencies = [
        ("kafka-python", "Kafka Python 客户端"),
        ("confluent-kafka", "Confluent Kafka 客户端"),
        ("pymysql", "MySQL 连接器"),
        ("sqlalchemy", "SQL 工具包"),
    ]
    
    for package_name, description in dependencies:
        if package_name.lower() in installed_packages:
            # Try to get version if possible
            try:
                if package_name == "kafka-python":
                    import kafka
                    version = getattr(kafka, '__version__', '已安装')
                elif package_name == "confluent-kafka":
                    import confluent_kafka
                    version = getattr(confluent_kafka, '__version__', '已安装')
                elif package_name == "pymysql":
                    import pymysql
                    version = getattr(pymysql, '__version__', '已安装')
                elif package_name == "sqlalchemy":
                    import sqlalchemy
                    version = getattr(sqlalchemy, '__version__', '已安装')
                else:
                    version = "已安装"
                results.append(("OK", f"{description}", f"{package_name} {version}"))
            except Exception:
                results.append(("OK", f"{description}", f"{package_name} 已安装"))
        else:
            results.append(("WARN", f"{description}", f"{package_name} 未安装 (可选)"))
    
    return results


def check_maven() -> Tuple[str, str]:
    """Check Maven (optional for PyFlink)"""
    try:
        result = subprocess.run(
            ["mvn", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0]
            return "OK", version_line
        else:
            return "WARN", "Maven 未安装 (PyFlink 通常不需要)"
    except FileNotFoundError:
        return "WARN", "Maven 未安装 (PyFlink 通常不需要)"
    except Exception as e:
        return "WARN", f"Maven 检测失败: {e}"


def main():
    """Main function"""
    print("\n" + "=" * 70)
    print("  Flink 开发环境检测报告")
    print("=" * 70)
    
    # 1. Java 环境
    print_section("1. Java 环境检测")
    status, details = check_java()
    print_item("Java 版本", status, details)
    
    # 2. Python 环境
    print_section("2. Python 环境检测")
    status, details = check_python()
    print_item("Python 版本", status, details)
    
    # 3. PyFlink 模块
    print_section("3. PyFlink 模块检测")
    module_results = check_pyflink_modules()
    for status, label, details in module_results:
        print_item(label, status, details)
    
    # 4. PyFlink 关键类
    print_section("4. PyFlink 关键类检测")
    class_results = check_pyflink_classes()
    for status, label, details in class_results:
        print_item(label, status, details)
    
    # 5. PyFlink 运行时测试
    print_section("5. PyFlink 运行时测试")
    status, details = check_pyflink_runtime()
    print_item("执行环境创建", status, details)
    
    # 6. 其他依赖
    print_section("6. 其他 Python 依赖检测")
    dep_results = check_other_dependencies()
    for status, label, details in dep_results:
        print_item(label, status, details)
    
    # 7. Maven (可选)
    print_section("7. 构建工具检测 (可选)")
    status, details = check_maven()
    print_item("Maven", status, details)
    
    # 总结
    print_section("检测总结")
    print("""
Flink 开发环境要求:
  - Java: 8/11/17 (推荐 17)
  - Python: 3.8+ (推荐 3.11)
  - PyFlink: 1.17.0+ (当前: 2.2.0)
  
如果所有核心模块显示 ✓，则环境满足 Flink 开发要求。
    """)
    
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()

