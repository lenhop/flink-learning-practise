# -*- coding: utf-8 -*-
import pymysql
import time
import logging
import contextlib
import urllib.parse
import json
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple
from sqlalchemy import create_engine, text, pool
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from pymysql.err import MySQLError, OperationalError as PyMySQLOperationalError


class MysqlDB:
    """
    Enhanced MySQL operations class with comprehensive functionality

    Features:
    - Connection management with retry logic
    - Error handling and logging
    - Transaction management
    - Connection pooling
    - Batch operations
    - Query optimization
    - Security enhancements
    """

    def __init__(self, config=None, **kwargs):
        """
        Initialize MySQL client with enhanced configuration

        Args:
            config (dict): Configuration dictionary
            **kwargs: Direct configuration parameters
                user (str): MySQL username
                password (str): MySQL password
                host (str): MySQL host, default 'localhost'
                port (int): MySQL port, default 3306
                database (str): Database name
                charset (str): Character set, default 'utf8mb4'
                connect_timeout (int): Connection timeout, default 10
                read_timeout (int): Read timeout, default 30
                write_timeout (int): Write timeout, default 30
                autocommit (bool): Auto commit, default False
                pool_size (int): Connection pool size, default 5
                max_overflow (int): Max pool overflow, default 10
                pool_recycle (int): Pool recycle time, default 3600
                ssl_disabled (bool): Disable SSL, default False
        """
        # Setup logger
        self.logger = logging.getLogger(__name__)

        # Parse configuration
        if config:
            if kwargs.get('datasource'):
                config = config['datasources'][kwargs['datasource']]

            self.user = config['user']
            self.password = config['password']
            self.host = config['host']
            self.port = config.get('port', 3306)
            self.charset = config.get('charset', 'utf8mb4')
            self.database = config['database']
        else:
            self.user = kwargs['user']  # Fixed: use consistent attribute name
            self.password = kwargs['password']
            self.host = kwargs['host']
            self.port = kwargs.get('port', 3306)
            self.charset = kwargs.get('charset', 'utf8mb4')
            self.database = kwargs['database']

        # Enhanced connection parameters
        self.connect_timeout = kwargs.get('connect_timeout', 10)
        self.read_timeout = kwargs.get('read_timeout', 30)
        self.write_timeout = kwargs.get('write_timeout', 30)
        self.autocommit = kwargs.get('autocommit', False)
        self.pool_size = kwargs.get('pool_size', 5)
        self.max_overflow = kwargs.get('max_overflow', 10)
        self.pool_recycle = kwargs.get('pool_recycle', 3600)
        self.ssl_disabled = kwargs.get('ssl_disabled', False)

        # Create connection pool
        self._create_engine()

        # Test connection
        self._test_connection()

    def _create_engine(self):
        """Create SQLAlchemy engine with connection pool"""
        try:
            # Build connection URL
            connection_url = (
                f"mysql+pymysql://{self.user}:{urllib.parse.quote_plus(self.password)}"
                f"@{self.host}:{self.port}/{self.database}"
            )

            # Engine configuration
            engine_config = {
                'poolclass': pool.QueuePool,
                'pool_size': self.pool_size,
                'max_overflow': self.max_overflow,
                'pool_recycle': self.pool_recycle,
                'pool_pre_ping': True,  # Verify connections before use
                'echo': False,  # Set to True for SQL debugging
                'connect_args': {
                    'charset': self.charset,
                    'connect_timeout': self.connect_timeout,
                    'read_timeout': self.read_timeout,
                    'write_timeout': self.write_timeout,
                    'autocommit': self.autocommit,
                }
            }

            # SSL configuration
            if self.ssl_disabled:
                engine_config['connect_args']['ssl_disabled'] = True

            self.engine = create_engine(connection_url, **engine_config)

        except Exception as e:
            self.logger.error(f"Failed to create MySQL engine: {str(e)}")
            raise

    def _test_connection(self):
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info(f"Successfully connected to MySQL at {self.host}:{self.port}/{self.database}")
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {str(e)}")
            raise ConnectionError(f"Cannot connect to MySQL server: {str(e)}")

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute database operation with retry logic"""
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                return operation(*args, **kwargs)
            except (OperationalError, PyMySQLOperationalError) as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"MySQL operation failed (attempt {attempt + 1}): {str(e)}")
                    time.sleep(retry_delay * (attempt + 1))
                    # Try to recreate engine
                    try:
                        self._create_engine()
                    except:
                        pass
                else:
                    self.logger.error(f"MySQL operation failed after {max_retries} attempts: {str(e)}")
                    raise
            except (SQLAlchemyError, MySQLError) as e:
                self.logger.error(f"MySQL error: {str(e)}")
                raise

    @contextlib.contextmanager
    def get_connection(self, autocommit: Optional[bool] = None):
        """
        Get database connection with proper resource management

        Args:
            autocommit (bool): Override autocommit setting

        Yields:
            Connection object
        """
        conn = None
        try:
            conn = self.engine.connect()
            if autocommit is not None:
                conn = conn.execution_options(autocommit=autocommit)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    @contextlib.contextmanager
    def get_transaction(self):
        """
        Get database transaction with proper rollback handling

        Yields:
            Connection object with transaction
        """
        conn = None
        trans = None
        try:
            conn = self.engine.connect()
            trans = conn.begin()
            yield conn
            trans.commit()
        except Exception as e:
            if trans:
                trans.rollback()
            self.logger.error(f"Transaction error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    # Legacy method for backward compatibility
    @contextlib.contextmanager
    def connect_mysql(self):
        """Legacy connection method (deprecated, use get_connection instead)"""
        self.logger.warning("connect_mysql is deprecated, use get_connection instead")

        conn = None
        cursor = None
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                db=self.database,
                charset=self.charset,
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                write_timeout=self.write_timeout
            )
            cursor = conn.cursor()
            yield cursor
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Legacy connection error: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def execute_query(self, sql: str, params: Optional[Dict] = None, fetch_all: bool = True):
        """
        Execute SELECT query and return results

        Args:
            sql (str): SQL query
            params (dict): Query parameters
            fetch_all (bool): Fetch all results or return iterator

        Returns:
            Query results
        """
        try:
            def _execute():
                with self.get_connection() as conn:
                    result = conn.execute(text(sql), params or {})
                    if fetch_all:
                        return result.fetchall()
                    else:
                        return result

            return self._execute_with_retry(_execute)
        except Exception as e:
            self.logger.error(f"Failed to execute query: {str(e)}")
            raise

    def execute_query_df(self, sql: str, params: Optional[Dict] = None):
        """
        Execute SELECT query and return pandas DataFrame

        Args:
            sql (str): SQL query
            params (dict): Query parameters

        Returns:
            pandas.DataFrame: Query results as DataFrame
        """
        try:
            def _execute():
                with self.get_connection() as conn:
                    return pd.read_sql(text(sql), conn, params=params or {})

            return self._execute_with_retry(_execute)
        except Exception as e:
            self.logger.error(f"Failed to execute query to DataFrame: {str(e)}")
            raise

    # Legacy method for backward compatibility
    def run_select(self, sql: str, params: Optional[Dict] = None):
        """Legacy select method (deprecated, use execute_query_df instead)"""
        self.logger.warning("run_select is deprecated, use execute_query_df instead")
        return self.execute_query_df(sql, params)

    def execute_dml(self, sql: str, params: Optional[Dict] = None):
        """
        Execute DML (INSERT, UPDATE, DELETE) statement

        Args:
            sql (str): SQL statement
            params (dict): Statement parameters

        Returns:
            int: Number of affected rows
        """
        try:
            def _execute():
                with self.get_transaction() as conn:
                    result = conn.execute(text(sql), params or {})
                    return result.rowcount

            return self._execute_with_retry(_execute)
        except Exception as e:
            self.logger.error(f"Failed to execute DML: {str(e)}")
            raise

    def execute_ddl(self, sql: str):
        """
        Execute DDL (CREATE, ALTER, DROP) statement

        Args:
            sql (str): DDL statement

        Returns:
            bool: True if successful
        """
        try:
            def _execute():
                with self.get_connection(autocommit=True) as conn:
                    conn.execute(text(sql))
                    return True

            return self._execute_with_retry(_execute)
        except Exception as e:
            self.logger.error(f"Failed to execute DDL: {str(e)}")
            raise

    # Legacy method for backward compatibility
    def run_ddl_and_dml(self, sql: str):
        """Legacy DDL/DML method (deprecated, use execute_dml or execute_ddl instead)"""
        self.logger.warning("run_ddl_and_dml is deprecated, use execute_dml or execute_ddl instead")

        # Try to determine if it's DDL or DML
        sql_upper = sql.strip().upper()
        if any(sql_upper.startswith(cmd) for cmd in ['CREATE', 'ALTER', 'DROP']):
            return self.execute_ddl(sql)
        else:
            return self.execute_dml(sql)

    def execute_batch_dml(self, sql: str, params_list: List[Dict]):
        """
        Execute batch DML operations

        Args:
            sql (str): SQL statement with parameters
            params_list (List[Dict]): List of parameter dictionaries

        Returns:
            int: Total number of affected rows
        """
        try:
            def _execute():
                total_affected = 0
                with self.get_transaction() as conn:
                    for params in params_list:
                        # Convert nested dicts and complex objects to JSON strings
                        # to avoid PyMySQL "dict can not be used as parameter" error
                        cleaned_params = self._clean_params(params)
                        result = conn.execute(text(sql), cleaned_params)
                        total_affected += result.rowcount
                return total_affected

            return self._execute_with_retry(_execute)
        except Exception as e:
            self.logger.error(f"Failed to execute batch DML: {str(e)}")
            raise

    def _clean_params(self, params: Dict):
        """
        Clean parameters by converting nested dicts and complex objects to JSON strings
        
        Args:
            params (Dict): Parameter dictionary
            
        Returns:
            Dict: Cleaned parameter dictionary with all values as basic types
        """
        cleaned = {}
        for key, value in params.items():
            if value is None:
                cleaned[key] = None
            elif isinstance(value, (str, int, float, bool)):
                cleaned[key] = value
            elif isinstance(value, dict):
                # Convert nested dict to JSON string
                cleaned[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, (list, tuple)):
                # Convert list/tuple to JSON string
                cleaned[key] = json.dumps(value, ensure_ascii=False)
            else:
                # Convert other complex objects to string
                cleaned[key] = str(value)
        return cleaned

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        schema: Optional[str] = None, if_exists: str = 'append',
                        index: bool = False, method: str = 'multi', chunksize: int = 1000):
        """
        Insert pandas DataFrame into database table

        Args:
            df (pd.DataFrame): DataFrame to insert
            table_name (str): Target table name
            schema (str): Schema name (optional)
            if_exists (str): How to behave if table exists ('fail', 'replace', 'append')
            index (bool): Write DataFrame index as column
            method (str): Insert method ('multi' for batch insert)
            chunksize (int): Number of rows per batch

        Returns:
            int: Number of rows inserted
        """
        try:
            def _execute():
                with self.get_connection() as conn:
                    rows_inserted = df.to_sql(
                        name=table_name,
                        con=conn,
                        schema=schema,
                        if_exists=if_exists,
                        index=index,
                        method=method,
                        chunksize=chunksize
                    )
                    return rows_inserted or len(df)

            return self._execute_with_retry(_execute)
        except Exception as e:
            self.logger.error(f"Failed to insert DataFrame: {str(e)}")
            raise

    # Legacy method for backward compatibility
    def load_df2db(self, df: pd.DataFrame, schema_name: str, table_name: str, if_exists: str = 'append'):
        """Legacy DataFrame insert method (deprecated, use insert_dataframe instead)"""
        self.logger.warning("load_df2db is deprecated, use insert_dataframe instead")
        return self.insert_dataframe(df, table_name, schema=schema_name, if_exists=if_exists)

    def get_table_info(self, table_name: str, schema: Optional[str] = None):
        """
        Get table structure information

        Args:
            table_name (str): Table name
            schema (str): Schema name (optional)

        Returns:
            pd.DataFrame: Table structure information
        """
        try:
            schema_clause = f"AND TABLE_SCHEMA = '{schema}'" if schema else ""
            sql = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_KEY,
                    EXTRA,
                    COLUMN_COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}' {schema_clause}
                ORDER BY ORDINAL_POSITION
            """
            return self.execute_query_df(sql)
        except Exception as e:
            self.logger.error(f"Failed to get table info: {str(e)}")
            raise

    def get_table_list(self, schema: Optional[str] = None):
        """
        Get list of tables in database

        Args:
            schema (str): Schema name (optional, defaults to current database)

        Returns:
            List[str]: List of table names
        """
        try:
            schema = schema or self.database
            sql = f"""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """
            result = self.execute_query_df(sql)
            return result['TABLE_NAME'].tolist()
        except Exception as e:
            self.logger.error(f"Failed to get table list: {str(e)}")
            raise

    def table_exists(self, table_name: str, schema: Optional[str] = None):
        """
        Check if table exists

        Args:
            table_name (str): Table name
            schema (str): Schema name (optional)

        Returns:
            bool: True if table exists
        """
        try:
            schema = schema or self.database
            sql = f"""
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table_name}'
            """
            result = self.execute_query_df(sql)
            return result.iloc[0]['count'] > 0
        except Exception as e:
            self.logger.error(f"Failed to check table existence: {str(e)}")
            return False

    def get_row_count(self, table_name: str, schema: Optional[str] = None, where_clause: str = ""):
        """
        Get row count for table

        Args:
            table_name (str): Table name
            schema (str): Schema name (optional)
            where_clause (str): WHERE clause (optional)

        Returns:
            int: Number of rows
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            where_part = f" WHERE {where_clause}" if where_clause else ""
            sql = f"SELECT COUNT(*) as count FROM {full_table_name}{where_part}"
            result = self.execute_query_df(sql)
            return result.iloc[0]['count']
        except Exception as e:
            self.logger.error(f"Failed to get row count: {str(e)}")
            raise

    def truncate_table(self, table_name: str, schema: Optional[str] = None):
        """
        Truncate table (remove all rows)

        Args:
            table_name (str): Table name
            schema (str): Schema name (optional)

        Returns:
            bool: True if successful
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            sql = f"TRUNCATE TABLE {full_table_name}"
            return self.execute_ddl(sql)
        except Exception as e:
            self.logger.error(f"Failed to truncate table: {str(e)}")
            raise

    def backup_table(self, source_table: str, backup_table: str, 
                    schema: Optional[str] = None, where_clause: str = ""):
        """
        Create backup of table

        Args:
            source_table (str): Source table name
            backup_table (str): Backup table name
            schema (str): Schema name (optional)
            where_clause (str): WHERE clause for selective backup

        Returns:
            int: Number of rows backed up
        """
        try:
            schema_prefix = f"{schema}." if schema else ""
            where_part = f" WHERE {where_clause}" if where_clause else ""

            # Create backup table
            create_sql = f"""
                CREATE TABLE {schema_prefix}{backup_table} 
                AS SELECT * FROM {schema_prefix}{source_table}{where_part}
            """
            self.execute_ddl(create_sql)

            # Return row count
            return self.get_row_count(backup_table, schema)
        except Exception as e:
            self.logger.error(f"Failed to backup table: {str(e)}")
            raise

    def optimize_table(self, table_name: str, schema: Optional[str] = None):
        """
        Optimize table performance

        Args:
            table_name (str): Table name
            schema (str): Schema name (optional)

        Returns:
            bool: True if successful
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            sql = f"OPTIMIZE TABLE {full_table_name}"
            return self.execute_ddl(sql)
        except Exception as e:
            self.logger.error(f"Failed to optimize table: {str(e)}")
            raise

    def analyze_table(self, table_name: str, schema: Optional[str] = None):
        """
        Analyze table for query optimization

        Args:
            table_name (str): Table name
            schema (str): Schema name (optional)

        Returns:
            bool: True if successful
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            sql = f"ANALYZE TABLE {full_table_name}"
            return self.execute_ddl(sql)
        except Exception as e:
            self.logger.error(f"Failed to analyze table: {str(e)}")
            raise

    def get_database_size(self, schema: Optional[str] = None):
        """
        Get database size information

        Args:
            schema (str): Schema name (optional)

        Returns:
            pd.DataFrame: Database size information
        """
        try:
            schema = schema or self.database
            sql = f"""
                SELECT 
                    table_schema as 'Database',
                    table_name as 'Table',
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) as 'Size_MB'
                FROM information_schema.TABLES 
                WHERE table_schema = '{schema}'
                ORDER BY (data_length + index_length) DESC
            """
            return self.execute_query_df(sql)
        except Exception as e:
            self.logger.error(f"Failed to get database size: {str(e)}")
            raise

    def test_connection(self):
        """
        Test database connection

        Returns:
            bool: True if connection successful
        """
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False

    def get_server_info(self):
        """
        Get MySQL server information

        Returns:
            Dict: Server information
        """
        try:
            sql = """
                SELECT 
                    @@version as version,
                    @@version_comment as version_comment,
                    @@character_set_server as charset,
                    @@collation_server as collation,
                    @@max_connections as max_connections,
                    @@innodb_buffer_pool_size as buffer_pool_size
            """
            result = self.execute_query_df(sql)
            return result.iloc[0].to_dict()
        except Exception as e:
            self.logger.error(f"Failed to get server info: {str(e)}")
            raise

    def close(self):
        """Close database engine and connections"""
        try:
            if hasattr(self, 'engine'):
                self.engine.dispose()
            self.logger.info("MySQL connection pool closed")
        except Exception as e:
            self.logger.error(f"Error closing MySQL connections: {str(e)}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Legacy function for backward compatibility
def get_db_helper(**kwargs):
    """Legacy helper function (deprecated, use MysqlDB directly)"""
    logging.getLogger(__name__).warning("get_db_helper is deprecated, use MysqlDB directly")
    return MysqlDB(**kwargs)


# Usage examples and common patterns
if __name__ == '__main__':
    """
    Comprehensive usage examples for MysqlDB
    """

    # Example 1: Basic connection and operations
    print("=== Example 1: Basic Operations ===")
    try:
        # Initialize MySQL client
        mysql_db = MysqlDB(
            user='your_username',
            password='your_password',
            host='localhost',
            port=3306,
            database='test_db',
            charset='utf8mb4'
        )

        # Test connection
        if mysql_db.test_connection():
            print("✓ MySQL connection successful")

        # Get server information
        server_info = mysql_db.get_server_info()
        print(f"MySQL Version: {server_info['version']}")

        mysql_db.close()

    except Exception as e:
        print(f"Error in basic operations: {str(e)}")

    # Example 2: Query operations
    print("\n=== Example 2: Query Operations ===")
    try:
        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            # Execute SELECT query and get DataFrame
            df = db.execute_query_df("SELECT * FROM users WHERE age > :age", {'age': 18})
            print(f"Found {len(df)} users over 18")

            # Execute raw query
            results = db.execute_query("SELECT COUNT(*) as total FROM users")
            print(f"Total users: {results[0]['total']}")

    except Exception as e:
        print(f"Error in query operations: {str(e)}")

    # Example 3: DML operations (INSERT, UPDATE, DELETE)
    print("\n=== Example 3: DML Operations ===")
    try:
        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            # Insert single record
            affected_rows = db.execute_dml(
                "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
                {'name': 'John Doe', 'email': 'john@example.com', 'age': 25}
            )
            print(f"Inserted {affected_rows} row(s)")

            # Update records
            affected_rows = db.execute_dml(
                "UPDATE users SET age = :new_age WHERE name = :name",
                {'new_age': 26, 'name': 'John Doe'}
            )
            print(f"Updated {affected_rows} row(s)")

            # Delete records
            affected_rows = db.execute_dml(
                "DELETE FROM users WHERE age > :max_age",
                {'max_age': 100}
            )
            print(f"Deleted {affected_rows} row(s)")

    except Exception as e:
        print(f"Error in DML operations: {str(e)}")

    # Example 4: Batch operations
    print("\n=== Example 4: Batch Operations ===")
    try:
        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            # Batch insert
            users_data = [
                {'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
                {'name': 'Bob', 'email': 'bob@example.com', 'age': 25},
                {'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35}
            ]

            total_affected = db.execute_batch_dml(
                "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
                users_data
            )
            print(f"Batch inserted {total_affected} row(s)")

    except Exception as e:
        print(f"Error in batch operations: {str(e)}")

    # Example 5: DataFrame operations
    print("\n=== Example 5: DataFrame Operations ===")
    try:
        import pandas as pd

        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            # Create sample DataFrame
            df = pd.DataFrame({
                'product_name': ['Laptop', 'Mouse', 'Keyboard'],
                'price': [999.99, 29.99, 79.99],
                'category': ['Electronics', 'Electronics', 'Electronics']
            })

            # Insert DataFrame to database
            rows_inserted = db.insert_dataframe(
                df, 
                table_name='products',
                if_exists='append',
                chunksize=1000
            )
            print(f"Inserted {rows_inserted} rows from DataFrame")

            # Read data back as DataFrame
            result_df = db.execute_query_df("SELECT * FROM products")
            print(f"Retrieved {len(result_df)} products")

    except Exception as e:
        print(f"Error in DataFrame operations: {str(e)}")

    # Example 6: DDL operations
    print("\n=== Example 6: DDL Operations ===")
    try:
        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            # Create table
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS temp_table (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            if db.execute_ddl(create_table_sql):
                print("✓ Table created successfully")

            # Check if table exists
            if db.table_exists('temp_table'):
                print("✓ Table exists")

            # Get table information
            table_info = db.get_table_info('temp_table')
            print(f"Table has {len(table_info)} columns")

            # Drop table
            if db.execute_ddl("DROP TABLE IF EXISTS temp_table"):
                print("✓ Table dropped successfully")

    except Exception as e:
        print(f"Error in DDL operations: {str(e)}")

    # Example 7: Table management
    print("\n=== Example 7: Table Management ===")
    try:
        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            # Get list of all tables
            tables = db.get_table_list()
            print(f"Database has {len(tables)} tables: {tables}")

            # Get row count for a table
            if 'users' in tables:
                row_count = db.get_row_count('users')
                print(f"Users table has {row_count} rows")

                # Get row count with condition
                adult_count = db.get_row_count('users', where_clause='age >= 18')
                print(f"Adult users: {adult_count}")

            # Get database size information
            size_info = db.get_database_size()
            print("Database size information:")
            print(size_info.head())

    except Exception as e:
        print(f"Error in table management: {str(e)}")

    # Example 8: Transaction management
    print("\n=== Example 8: Transaction Management ===")
    try:
        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            # Manual transaction management
            with db.get_transaction() as conn:
                # Multiple operations in single transaction
                conn.execute(text("INSERT INTO users (name, email, age) VALUES ('Test1', 'test1@example.com', 20)"))
                conn.execute(text("INSERT INTO users (name, email, age) VALUES ('Test2', 'test2@example.com', 21)"))
                # Transaction will be committed automatically if no exception
                print("✓ Transaction completed successfully")

    except Exception as e:
        print(f"Error in transaction management: {str(e)}")

    # Example 9: Table maintenance
    print("\n=== Example 9: Table Maintenance ===")
    try:
        with MysqlDB(user='root', password='password', host='localhost', database='test_db') as db:
            table_name = 'users'

            # Analyze table for optimization
            if db.analyze_table(table_name):
                print(f"✓ Analyzed table {table_name}")

            # Optimize table
            if db.optimize_table(table_name):
                print(f"✓ Optimized table {table_name}")

            # Create backup of table
            backup_rows = db.backup_table(
                source_table=table_name,
                backup_table=f"{table_name}_backup",
                where_clause="created_at >= '2024-01-01'"
            )
            print(f"✓ Backed up {backup_rows} rows")

    except Exception as e:
        print(f"Error in table maintenance: {str(e)}")

    # Example 10: Configuration-based initialization
    print("\n=== Example 10: Configuration-based Setup ===")
    try:
        # Configuration dictionary
        config = {
            'datasources': {
                'primary': {
                    'user': 'root',
                    'password': 'password',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_db',
                    'charset': 'utf8mb4'
                },
                'readonly': {
                    'user': 'readonly_user',
                    'password': 'readonly_pass',
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'test_db',
                    'charset': 'utf8mb4'
                }
            }
        }

        # Initialize with configuration
        with MysqlDB(config=config, datasource='primary') as primary_db:
            if primary_db.test_connection():
                print("✓ Primary database connection successful")

        with MysqlDB(config=config, datasource='readonly') as readonly_db:
            if readonly_db.test_connection():
                print("✓ Readonly database connection successful")

    except Exception as e:
        print(f"Error in configuration-based setup: {str(e)}")

    print("\n=== All Examples Completed ===")
    print("Note: Make sure MySQL server is running and credentials are correct for examples to work")
