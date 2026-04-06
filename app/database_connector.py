"""
Database Connection Manager for DW Migration Assistant
Handles JDBC and native connections to various source databases
"""

import os
import logging
from contextlib import contextmanager
from typing import Dict, Any, List, Generator, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import database connectors with graceful fallback
try:
    import jaydebeapi
    JAYDEBEAPI_AVAILABLE = True
except ImportError:
    JAYDEBEAPI_AVAILABLE = False
    jaydebeapi = None

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    psycopg2 = None

try:
    import pymysql
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False
    pymysql = None

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None


class DatabaseConnectionManager:
    """Manages database connections with connection pooling and timeout handling"""

    @staticmethod
    @contextmanager
    def get_connection(conn_info: Dict[str, Any], timeout: int = 30) -> Generator:
        """
        Create a database connection based on source type
        Supports: Oracle (JDBC), SQL Server (JDBC/pyodbc), MySQL (pymysql),
        PostgreSQL/Redshift (psycopg2), Snowflake (snowflake-connector)
        """
        source_type = conn_info["source_type"]
        conn = None

        try:
            logger.info(f"Attempting connection to {source_type} at {conn_info['host']}:{conn_info['port']}")

            if source_type == "mysql" and PYMYSQL_AVAILABLE:
                # Use PyMySQL for MySQL
                conn = pymysql.connect(
                    host=conn_info["host"],
                    port=conn_info["port"],
                    user=conn_info["username"],
                    password=conn_info["password"],
                    database=conn_info["database"],
                    connect_timeout=timeout,
                    read_timeout=timeout,
                    write_timeout=timeout
                )
                logger.info(f"Successfully connected to MySQL using pymysql")

            elif source_type in ["redshift", "postgres"] and PSYCOPG2_AVAILABLE:
                # Use psycopg2 for PostgreSQL/Redshift
                conn = psycopg2.connect(
                    host=conn_info["host"],
                    port=conn_info["port"],
                    user=conn_info["username"],
                    password=conn_info["password"],
                    database=conn_info["database"],
                    connect_timeout=timeout
                )
                logger.info(f"Successfully connected to {source_type} using psycopg2")

            elif source_type == "snowflake" and SNOWFLAKE_AVAILABLE:
                # Use Snowflake connector
                additional_params = conn_info.get("additional_params", {})
                conn = snowflake.connector.connect(
                    user=conn_info["username"],
                    password=conn_info["password"],
                    account=conn_info["host"],  # Snowflake account identifier
                    warehouse=additional_params.get("warehouse", "COMPUTE_WH"),
                    database=conn_info["database"],
                    login_timeout=timeout,
                    network_timeout=timeout
                )
                logger.info(f"Successfully connected to Snowflake")

            elif JAYDEBEAPI_AVAILABLE:
                # Use JayDeBeApi for JDBC connections (Oracle, SQL Server, Teradata, etc.)
                jdbc_url = get_jdbc_url(
                    source_type,
                    conn_info["host"],
                    conn_info["port"],
                    conn_info["database"],
                    conn_info.get("additional_params")
                )

                driver_class = get_jdbc_driver_class(source_type)
                driver_path = get_jdbc_driver_path(source_type)

                if not driver_path or not os.path.exists(driver_path):
                    logger.warning(f"JDBC driver not found for {source_type} at {driver_path}")
                    raise Exception(f"JDBC driver not found for {source_type}. Please install the driver at {driver_path}")

                conn = jaydebeapi.connect(
                    driver_class,
                    jdbc_url,
                    [conn_info["username"], conn_info["password"]],
                    driver_path
                )
                logger.info(f"Successfully connected to {source_type} using JDBC")
            else:
                raise Exception(f"No suitable connector available for {source_type}. Please install required libraries.")

            yield conn

        except Exception as e:
            logger.error(f"Connection error for {source_type}: {str(e)}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                    logger.info(f"Connection to {source_type} closed")
                except:
                    pass

    @staticmethod
    def test_connection(conn_info: Dict[str, Any]) -> Tuple[bool, str]:
        """Test database connection"""
        try:
            with DatabaseConnectionManager.get_connection(conn_info, timeout=10) as conn:
                cursor = conn.cursor()

                # Use appropriate test query based on source type
                test_query = "SELECT 1"
                if conn_info["source_type"] == "oracle":
                    test_query = "SELECT 1 FROM DUAL"

                cursor.execute(test_query)
                result = cursor.fetchone()
                cursor.close()

                if result:
                    return True, f"Successfully connected to {conn_info['source_type']} at {conn_info['host']}:{conn_info['port']}"
                else:
                    return False, "Connection test query failed"
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False, str(e)

    @staticmethod
    def execute_query(conn_info: Dict[str, Any], query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries"""
        try:
            with DatabaseConnectionManager.get_connection(conn_info) as conn:
                cursor = conn.cursor()

                # Handle parameterized queries
                if params:
                    # Convert dict params to positional or named based on source type
                    source_type = conn_info["source_type"]
                    if source_type in ["oracle", "teradata"]:
                        # Oracle/Teradata use :param_name syntax
                        cursor.execute(query, params)
                    elif source_type in ["sqlserver", "synapse"]:
                        # SQL Server uses @param_name syntax
                        cursor.execute(query, params)
                    else:
                        # PostgreSQL, MySQL, Redshift use %s or %(name)s
                        cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                # Fetch all rows
                rows = cursor.fetchall()
                cursor.close()

                # Convert to list of dicts
                results = []
                for row in rows:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        # Handle various data types
                        value = row[i]
                        # Convert special types to strings for JSON serialization
                        if hasattr(value, '__class__') and value.__class__.__name__ in ['LOB', 'CLOB', 'BLOB']:
                            value = str(value)
                        row_dict[col] = value
                    results.append(row_dict)

                logger.info(f"Query executed successfully, returned {len(results)} rows")
                return results

        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            raise


def get_jdbc_driver_class(source_type: str) -> str:
    """Get JDBC driver class for each source system"""
    drivers = {
        "oracle": "oracle.jdbc.driver.OracleDriver",
        "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "teradata": "com.teradata.jdbc.TeraDriver",
        "netezza": "org.netezza.Driver",
        "synapse": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return drivers.get(source_type, "")


def get_jdbc_driver_path(source_type: str) -> str:
    """Get JDBC driver JAR file path for each source system"""
    # In production, these should be configured via environment variables
    driver_base_path = os.getenv("JDBC_DRIVERS_PATH", "/opt/jdbc_drivers")
    drivers = {
        "oracle": f"{driver_base_path}/ojdbc8.jar",
        "sqlserver": f"{driver_base_path}/mssql-jdbc.jar",
        "teradata": f"{driver_base_path}/terajdbc4.jar",
        "netezza": f"{driver_base_path}/nzjdbc.jar",
        "synapse": f"{driver_base_path}/mssql-jdbc.jar"
    }
    return drivers.get(source_type, "")


def get_jdbc_url(source_type: str, host: str, port: int, database: str, additional_params: Optional[Dict] = None) -> str:
    """Generate JDBC URL for different source systems"""
    params_str = ""
    if additional_params:
        params_str = "&".join([f"{k}={v}" for k, v in additional_params.items()])

    jdbc_urls = {
        "oracle": f"jdbc:oracle:thin:@{host}:{port}/{database}",
        "snowflake": f"jdbc:snowflake://{host}/?db={database}&warehouse={additional_params.get('warehouse', 'COMPUTE_WH') if additional_params else 'COMPUTE_WH'}",
        "sqlserver": f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true",
        "teradata": f"jdbc:teradata://{host}/DATABASE={database}",
        "netezza": f"jdbc:netezza://{host}:{port}/{database}",
        "synapse": f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true",
        "redshift": f"jdbc:redshift://{host}:{port}/{database}",
        "mysql": f"jdbc:mysql://{host}:{port}/{database}?useSSL=true"
    }
    return jdbc_urls.get(source_type, "")


def get_default_port(source_type: str) -> int:
    """Get default port for each source system"""
    ports = {
        "oracle": 1521,
        "snowflake": 443,
        "sqlserver": 1433,
        "teradata": 1025,
        "netezza": 5480,
        "synapse": 1433,
        "redshift": 5439,
        "mysql": 3306
    }
    return ports.get(source_type, 0)
