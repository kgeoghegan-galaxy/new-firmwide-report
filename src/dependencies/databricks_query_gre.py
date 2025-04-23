from databricks import sql
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import Optional

class DatabricksGREQuery:
    def __init__(self):
        """Initialize DatabricksGREQuery with connection parameters from environment."""
        load_dotenv()
        
        self.databricks_pat = os.getenv("DATABRICKS_PAT")
        if not self.databricks_pat:
            raise ValueError("DATABRICKS_PAT not found in environment variables")
            
        self.server_hostname = "gdt-mo.cloud.databricks.com"
        self.http_path = "sql/protocolv1/o/2455603699819334/0227-183543-al5o1v9b"
        
    def get_connection(self):
        """Create and return a new Databricks SQL connection."""
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.databricks_pat
        )
        
    def get_gre_positions(self, trading_day: str) -> pd.DataFrame:
        """
        Fetch GRE position data for a specific trading day.
        
        Args:
            trading_day (str): Date in 'YYYY-MM-DD' format
            
        Returns:
            pd.DataFrame: DataFrame containing GRE positions for the specified date
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            query = f"""
            SELECT *
            FROM gc_accounting.finance_uat.dt_gre_pnl_snapshot
            WHERE TRADING_DAY = '{trading_day}'
            ORDER BY TRADING_DAY DESC
            LIMIT 1000;
            """
            
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            
            df = pd.DataFrame(data, columns=columns)
            print(f"Retrieved {len(df)} rows of GRE position data for {trading_day}")
            
            return df
            
        finally:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # Example usage
    query = DatabricksGREQuery()
    df = query.get_gre_positions("2025-01-17")