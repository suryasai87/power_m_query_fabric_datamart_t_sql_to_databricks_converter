"""
Databricks client utility for SQL execution and testing.

Uses the DEFAULT profile from ~/.databrickscfg
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementResponse
import logging
from typing import Optional, List, Dict, Any
from config import DATABRICKS_PROFILE, WAREHOUSE_ID

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksClient:
    """
    Client for interacting with Databricks SQL Serverless Warehouse.
    """

    def __init__(self, profile: str = DATABRICKS_PROFILE, warehouse_id: str = WAREHOUSE_ID):
        """
        Initialize Databricks client.

        Args:
            profile: Databricks profile name from ~/.databrickscfg (default: DEFAULT)
            warehouse_id: SQL Serverless Warehouse ID
        """
        self.profile = profile
        self.warehouse_id = warehouse_id

        try:
            self.client = WorkspaceClient(profile=profile)
            logger.info(f"Connected to Databricks using profile: {profile}")
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            raise

    def execute_sql(self, query: str, catalog: Optional[str] = None,
                   schema: Optional[str] = None) -> ExecuteStatementResponse:
        """
        Execute SQL query on Databricks SQL Serverless Warehouse.

        Args:
            query: SQL query to execute
            catalog: Optional catalog name
            schema: Optional schema name

        Returns:
            ExecuteStatementResponse object
        """
        try:
            logger.info(f"Executing SQL query...")
            logger.debug(f"Query: {query[:100]}...")

            response = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=query,
                catalog=catalog,
                schema=schema,
                wait_timeout="30s"
            )

            logger.info(f"Query executed successfully. Status: {response.status.state}")
            return response

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test Databricks connection with a simple query.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            test_query = "SELECT 1 as test_value, current_timestamp() as test_time"
            response = self.execute_sql(test_query)

            if response.status.state == "SUCCEEDED":
                logger.info("✓ Connection test PASSED")
                logger.info(f"  Warehouse ID: {self.warehouse_id}")
                logger.info(f"  Profile: {self.profile}")
                return True
            else:
                logger.warning(f"Connection test returned status: {response.status.state}")
                return False

        except Exception as e:
            logger.error(f"✗ Connection test FAILED: {e}")
            return False

    def validate_sql(self, query: str) -> Dict[str, Any]:
        """
        Validate SQL syntax without executing the full query.

        Args:
            query: SQL query to validate

        Returns:
            Dictionary with validation results
        """
        try:
            # Use EXPLAIN to validate syntax
            explain_query = f"EXPLAIN {query}"
            response = self.execute_sql(explain_query)

            return {
                "valid": response.status.state == "SUCCEEDED",
                "status": response.status.state,
                "message": "SQL syntax is valid"
            }

        except Exception as e:
            return {
                "valid": False,
                "status": "FAILED",
                "message": str(e)
            }

    def get_warehouse_info(self) -> Dict[str, Any]:
        """
        Get information about the SQL Serverless Warehouse.

        Returns:
            Dictionary with warehouse information
        """
        try:
            warehouse = self.client.warehouses.get(self.warehouse_id)

            return {
                "id": warehouse.id,
                "name": warehouse.name,
                "size": warehouse.cluster_size,
                "state": warehouse.state,
                "warehouse_type": warehouse.warehouse_type
            }

        except Exception as e:
            logger.error(f"Failed to get warehouse info: {e}")
            return {}


def test_databricks_connection():
    """
    Quick test function to verify Databricks connection.
    """
    print("=" * 60)
    print("Databricks Connection Test")
    print("=" * 60)

    try:
        client = DatabricksClient()

        # Test connection
        if client.test_connection():
            print("\n✓ Connection successful!")

            # Get warehouse info
            warehouse_info = client.get_warehouse_info()
            if warehouse_info:
                print(f"\nWarehouse Information:")
                print(f"  Name: {warehouse_info.get('name')}")
                print(f"  ID: {warehouse_info.get('id')}")
                print(f"  Size: {warehouse_info.get('size')}")
                print(f"  State: {warehouse_info.get('state')}")
                print(f"  Type: {warehouse_info.get('warehouse_type')}")
        else:
            print("\n✗ Connection failed!")

    except Exception as e:
        print(f"\n✗ Error: {e}")

    print("=" * 60)


if __name__ == "__main__":
    test_databricks_connection()
