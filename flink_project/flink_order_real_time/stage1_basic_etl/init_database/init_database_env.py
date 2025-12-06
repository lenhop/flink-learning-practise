#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Initialize MySQL database schema for Walmart order pipeline

This script will:
1. Read configuration from table_config.yml in init_database directory
2. Connect to MySQL database
3. Create schemas as specified in config
4. Create tables as specified in config (from SQL files in sql directory)

Usage:
    python init_database_env.py
"""

import os
import sys
import copy
import argparse
import pymysql
import yaml

# add the parent directory to the sys.path
current_file_path = os.path.abspath(__file__)
target_directory_name = 'flink-learning-practise'

if f'/{target_directory_name}/' not in current_file_path and \
   f'\\{target_directory_name}\\' not in current_file_path:
    target_directory_path = copy.deepcopy(current_file_path)
    for _ in range(10):
        target_directory_path = os.path.dirname(target_directory_path)
        if str(target_directory_path).endswith(target_directory_name):
            break
    sys.path.append(target_directory_path)
else:
    # Extract the directory path from current_file_path
    target_directory_path = current_file_path
    for _ in range(10):
        target_directory_path = os.path.dirname(target_directory_path)
        if str(target_directory_path).endswith(target_directory_name):
            break
    sys.path.append(target_directory_path)

try:
    from flink_project.config.config import BaseConfig
except ImportError:
    print("Error: Cannot import BaseConfig from flink_project.config.config")
    print("Make sure the project structure is correct")
    sys.exit(1)


def get_mysql_config():
    """Get MySQL configuration from config.yml file"""
    try:
        # Get the project root directory
        script_dir = os.path.dirname(current_file_path)
        # Navigate from init_database to stage1_basic_etl to flink_order_real_time to flink_project to config
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
        config_file = os.path.join(project_root, 'config', 'config.yml')
        
        # Load configuration from config.yml
        if not os.path.exists(config_file):
            print(f"Warning: Config file not found: {config_file}")
            print("Using default MySQL configuration for Docker environment")
            return get_default_mysql_config()
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if 'mysql' not in config:
            print("Warning: MySQL configuration not found in config.yml")
            print("Using default MySQL configuration for Docker environment")
            return get_default_mysql_config()
        
        print(f"✓ Loaded MySQL configuration from: {config_file}")
        return config['mysql']
    
    except Exception as e:
        print(f"Error loading MySQL configuration: {str(e)}")
        print("Using default MySQL configuration for Docker environment")
        return get_default_mysql_config()


def get_default_mysql_config():
    """Get default MySQL configuration for Docker environment"""
    return {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root123456',
        'charset': 'utf8mb4',
    }


def load_init_config(config_file_path):
    """
    Load initialization configuration from YAML file

    Args:
        config_file_path: Path to table_config.yml file

    Returns:
        dict: Configuration dictionary with 'schemas' and 'tables' keys
    """
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(
            f"Config file not found: {config_file_path}"
        )

    with open(config_file_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    if 'init_config' not in config:
        raise ValueError(
            "Invalid config file: 'init_config' key not found"
        )

    init_config = config['init_config']

    # Extract schemas (handle typo 'scheam' in config)
    schemas = []
    if 'scheam' in init_config:  # Handle typo
        schemas = [
            item.get('name')
            for item in init_config['scheam']
            if 'name' in item
        ]
    elif 'schema' in init_config:
        schemas = [
            item.get('name')
            for item in init_config['schema']
            if 'name' in item
        ]

    # Extract tables
    tables = []
    if 'table' in init_config:
        tables = [
            item.get('name')
            for item in init_config['table']
            if 'name' in item
        ]

    return {
        'schemas': schemas,
        'tables': tables
    }


def find_sql_file(sql_dir, schema_table_name):
    """
    Find SQL file for given schema.table_name

    Args:
        sql_dir: Directory containing SQL files
        schema_table_name: Table name in format 'schema.table_name'

    Returns:
        str: Path to SQL file, or None if not found
    """
    # Extract schema and table name
    schema_name, table_name = schema_table_name.split('.', 1)
    
    # Try different naming conventions
    possible_names = [
        f"{schema_table_name}.sql",  # schema.table_name.sql
        f"create_{table_name}.sql",   # create_table_name.sql
        f"{table_name}.sql"           # table_name.sql
    ]
    
    for name in possible_names:
        sql_file_path = os.path.join(sql_dir, name)
        if os.path.exists(sql_file_path):
            return sql_file_path
    
    return None


def read_sql_file(file_path):
    """Read SQL file content"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()


def database_exists(cursor, database_name):
    """Check if database exists"""
    cursor.execute("SHOW DATABASES LIKE %s", (database_name,))
    return cursor.fetchone() is not None


def table_exists(cursor, database_name, table_name):
    """Check if table exists in database"""
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (database_name, table_name))
    return cursor.fetchone()[0] > 0


def create_database(cursor, database_name):
    """Create database if not exists"""
    sql = (
        f"CREATE DATABASE IF NOT EXISTS {database_name} "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    cursor.execute(sql)
    print(f"✓ Database '{database_name}' created or already exists")


def execute_sql_file(cursor, sql_content, description):
    """Execute SQL file content"""
    # Remove comments and split by semicolon
    lines = sql_content.split('\n')
    cleaned_lines = []
    for line in lines:
        # Remove inline comments
        if '--' in line:
            line = line[:line.index('--')]
        cleaned_lines.append(line)

    # Join and split by semicolon
    cleaned_content = '\n'.join(cleaned_lines)
    statements = [
        stmt.strip()
        for stmt in cleaned_content.split(';')
        if stmt.strip()
    ]

    for statement in statements:
        # Skip empty statements
        if statement:
            try:
                cursor.execute(statement)
                print(f"✓ {description}")
            except Exception as e:
                # If table already exists, that's okay
                error_msg = str(e).lower()
                if ("already exists" in error_msg or
                        "duplicate" in error_msg or
                        "1050" in error_msg):
                    print(f"✓ {description} (already exists)")
                else:
                    print(f"✗ Error executing SQL: {str(e)}")
                    raise


def init_database_schema():
    """
    Initialize database schema from table_config.yml file
    """
    print("=" * 60)
    print("MySQL Database Schema Initialization")
    print("=" * 60)

    # Get config file path (same directory as this script)
    script_dir = os.path.dirname(current_file_path)
    config_file = os.path.join(script_dir, 'table_config.yml')

    # Load configuration
    print(f"\nLoading configuration from: {config_file}")
    try:
        config = load_init_config(config_file)
        schemas = config['schemas']
        tables = config['tables']
        schemas_str = ', '.join(schemas)
        tables_str = ', '.join(tables)
        print(f"✓ Found {len(schemas)} schema(s) to create: {schemas_str}")
        print(f"✓ Found {len(tables)} table(s) to create: {tables_str}")
    except Exception as e:
        print(f"✗ Error loading config file: {str(e)}")
        return False

    # Get MySQL config
    mysql_config = get_mysql_config()
    print("\nConnecting to MySQL...")
    print(f"Host: {mysql_config['host']}")
    print(f"Port: {mysql_config['port']}")
    print(f"User: {mysql_config['user']}")

    # Get SQL files directory
    # (init_database -> sql)
    project_root = os.path.dirname(script_dir)
    sql_dir = os.path.join(project_root, 'sql')
    print(f"SQL directory: {sql_dir}")

    connection = None
    try:
        # Connect to MySQL (without specifying database)
        connection = pymysql.connect(
            host=mysql_config['host'],
            port=mysql_config['port'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            charset=mysql_config['charset'],
            connect_timeout=10
        )
        print("✓ Connected to MySQL server")

        cursor = connection.cursor()

        # Step 1: Create schemas
        if schemas:
            print("\n[Step 1] Creating schemas...")
            for schema_name in schemas:
                print(f"  Checking schema '{schema_name}'...")
                if not database_exists(cursor, schema_name):
                    msg = f"  Schema '{schema_name}' does not exist, "
                    msg += "creating..."
                    print(msg)
                    create_database(cursor, schema_name)
                else:
                    print(f"  ✓ Schema '{schema_name}' already exists")
            connection.commit()
        else:
            print("\n[Step 1] No schemas to create")

        # Step 2: Create tables
        if tables:
            print("\n[Step 2] Creating tables...")
            for schema_table_name in tables:
                # Parse schema.table_name
                if '.' not in schema_table_name:
                    msg = (f"  ✗ Invalid table name format: "
                           f"{schema_table_name} "
                           f"(expected: schema.table_name)")
                    print(msg)
                    continue

                schema_name, table_name = schema_table_name.split('.', 1)
                print(f"  Checking table '{schema_table_name}'...")

                # Check if table exists
                if not table_exists(cursor, schema_name, table_name):
                    msg = (f"  Table '{schema_table_name}' "
                           f"does not exist, creating...")
                    print(msg)

                    # Find SQL file
                    sql_file = find_sql_file(sql_dir, schema_table_name)
                    if not sql_file:
                        msg = (f"  ✗ SQL file not found for "
                               f"'{schema_table_name}'")
                        print(msg)
                        expected = os.path.join(
                            sql_dir, f'{schema_table_name}.sql'
                        )
                        print(f"    Expected file: {expected}")
                        continue

                    # Read and execute SQL file
                    try:
                        sql_content = read_sql_file(sql_file)
                        desc = f"Table '{schema_table_name}' created"
                        execute_sql_file(cursor, sql_content, desc)
                        connection.commit()
                    except Exception as e:
                        msg = (f"  ✗ Error creating table "
                               f"'{schema_table_name}': {str(e)}")
                        print(msg)
                        connection.rollback()
                        continue
                else:
                    print(f"  ✓ Table '{schema_table_name}' already exists")
        else:
            print("\n[Step 2] No tables to create")

        # Verify final state
        print("\n" + "=" * 60)
        print("Verification:")
        print("=" * 60)

        for schema_name in schemas:
            cursor.execute("SHOW DATABASES LIKE %s", (schema_name,))
            if cursor.fetchone():
                print(f"✓ Schema '{schema_name}' exists")
                cursor.execute(f"USE {schema_name}")
                cursor.execute("SHOW TABLES")
                schema_tables = [row[0] for row in cursor.fetchall()]
                tables_str = (
                    ', '.join(schema_tables)
                    if schema_tables
                    else '(none)'
                )
                print(f"  Tables: {tables_str}")

        print("\n" + "=" * 60)
        print("Database schema initialization completed successfully!")
        print("=" * 60)
        return True

    except pymysql.Error as e:
        print("\n✗ MySQL Error occurred!")
        print(f"Error Code: {getattr(e, 'args', [None])[0]}")
        print(f"Error Message: {str(e)}")
        if connection:
            connection.rollback()
        return False

    except FileNotFoundError as e:
        print(f"\n✗ File Error: {str(e)}")
        return False

    except Exception as e:
        print("\n✗ Unexpected error occurred!")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        if connection:
            connection.rollback()
        return False

    finally:
        if connection:
            connection.close()
            print("\nConnection closed.")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Initialize MySQL database schema from table_config.yml',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python init_database_env.py

The script will look for table_config.yml in the init_database directory.
        """
    )

    args = parser.parse_args()
    success = init_database_schema()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()