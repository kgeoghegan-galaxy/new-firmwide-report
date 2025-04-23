from databricks import sql
import pandas as pd
import pytz
from datetime import datetime, timedelta
import os
from typing import List, Optional, Tuple, Union
import logging

class GDTDataManager:
    
    def __init__(
        self, 
        server_hostname: str = "gdt-mo.cloud.databricks.com",
        http_path: str = "sql/protocolv1/o/2455603699819334/0227-183543-al5o1v9b",
        access_token: str = None,
        table_name: str = "gc_risk.smile_delta_bm_testing.smile_delta_table_bm_testing",
        market_close_hour_utc: int = 22
    ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.table_name = table_name
        self.market_close_hour_utc = market_close_hour_utc
        
        if access_token is None:
            self.access_token = os.getenv("DATABRICKS_PAT")
            if self.access_token is None:
                raise ValueError("Databricks access token not provided and not found in environment")
        else:
            self.access_token = access_token
            
        self.logger = logging.getLogger(__name__)
        
    def _get_connection(self):
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        )
    
    def _identify_key_metrics(self, df: pd.DataFrame) -> dict:
        """Identify and extract only the key metrics we care about"""
        if df.empty:
            return {}
            
        # Based on the available columns, focus on the specific asset classes and Greeks
        key_metrics = {}
        
        # Capture timestamp first
        if 'Timestamp__UTC' in df.columns:
            utc_time = df['Timestamp__UTC'].iloc[0]
            est_time = utc_time.astimezone(pytz.timezone('US/Eastern'))
            key_metrics['Timestamp_EST'] = est_time
        
        # Spot prices
        spot_metrics = ['BTC_Spot', 'ETH_Spot']
        for col in spot_metrics:
            if col in df.columns:
                key_metrics[col] = df[col].iloc[0]
        
        # SOL data
        if 'SOL_BS_Delta' in df.columns:
            key_metrics['SOL_Delta'] = df['SOL_BS_Delta'].iloc[0]
        
        # Alt data  
        if 'Alt_Net_Delta' in df.columns:
            key_metrics['Alt_Delta'] = df['Alt_Net_Delta'].iloc[0]
        
        if 'Largest_Alt' in df.columns and 'Largest_Alt_Delta' in df.columns:
            key_metrics['Largest_Alt'] = df['Largest_Alt'].iloc[0]
            key_metrics['Largest_Alt_Delta'] = df['Largest_Alt_Delta'].iloc[0]
        
        # BTC Greeks
        btc_greek_mapping = {
            'Delta': 'BTC_BS_Delta_Net',
            'Gamma': 'BTC_Smile_Gamma',
            'Vega': 'BTC_BS_Vega',
            'Theta': 'BTC_BS_Theta'
        }
        
        for greek, column in btc_greek_mapping.items():
            if column in df.columns:
                key_metrics[f'BTC_{greek}'] = df[column].iloc[0]
        
        # ETH Greeks
        eth_greek_mapping = {
            'Delta': 'ETH_BS_Delta_Net',
            'Gamma': 'ETH_Smile_Gamma',
            'Vega': 'ETH_BS_Vega',
            'Theta': 'ETH_BS_Theta'
        }
        
        for greek, column in eth_greek_mapping.items():
            if column in df.columns:
                key_metrics[f'ETH_{greek}'] = df[column].iloc[0]
                
        # Calculate total Delta, Gamma, Vega, Theta across assets
        # Start with BTC and ETH values
        total_delta = key_metrics.get('BTC_Delta', 0) + key_metrics.get('ETH_Delta', 0)
        total_gamma = key_metrics.get('BTC_Gamma', 0) + key_metrics.get('ETH_Gamma', 0)
        total_vega = key_metrics.get('BTC_Vega', 0) + key_metrics.get('ETH_Vega', 0)
        total_theta = key_metrics.get('BTC_Theta', 0) + key_metrics.get('ETH_Theta', 0)
        
        # Add SOL and Alt deltas if available
        if 'SOL_Delta' in key_metrics:
            total_delta += key_metrics['SOL_Delta']
        
        if 'Alt_Delta' in key_metrics:
            total_delta += key_metrics['Alt_Delta']
        
        # Add totals to key metrics
        key_metrics['Total_Delta'] = total_delta
        key_metrics['Total_Gamma'] = total_gamma
        key_metrics['Total_Vega'] = total_vega
        key_metrics['Total_Theta'] = total_theta
            
        return key_metrics
    
    def _execute_query(self, query: str) -> pd.DataFrame:
        connection = None
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df
                
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return pd.DataFrame()
        finally:
            if connection:
                connection.close()
    
    def get_market_close_data(self, date: Union[str, datetime]) -> pd.DataFrame:
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d').date()
        elif isinstance(date, datetime):
            date = date.date()
            
        date_str = date.strftime('%Y-%m-%d')
        
        query = f"""
        WITH DailyCloseData AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    ORDER BY ABS(
                        EXTRACT(HOUR FROM Timestamp__UTC) * 3600 +
                        EXTRACT(MINUTE FROM Timestamp__UTC) * 60 +
                        EXTRACT(SECOND FROM Timestamp__UTC) -
                        {self.market_close_hour_utc} * 3600
                    ) ASC
                ) AS RowRank
            FROM {self.table_name}
            WHERE DATE(Timestamp__UTC) = '{date_str}'
        )
        SELECT *
        FROM DailyCloseData
        WHERE RowRank = 1;
        """
        
        return self._execute_query(query)
    
    def get_latest_market_close(self) -> Tuple[pd.DataFrame, datetime]:
        now = datetime.now(pytz.timezone('US/Eastern'))
        target_hour = 17
        
        if now.hour < target_hour:
            target_date = (now - timedelta(days=1)).date()
        else:
            target_date = now.date()
            
        df = self.get_market_close_data(target_date)
        return df, target_date
    
    def get_weekly_comparison(self) -> dict:
        """
        Get market close data for the most recent close and exactly one week prior.
        Returns a dictionary with metrics for both dates and percentage changes.
        """
        recent_df, recent_date = self.get_latest_market_close()
        
        week_ago_date = recent_date - timedelta(days=7)
        week_ago_df = self.get_market_close_data(week_ago_date)
        
        # Get key metrics for each date
        recent_metrics = self._identify_key_metrics(recent_df)
        week_ago_metrics = self._identify_key_metrics(week_ago_df)
        
        # Calculate percentage changes for numeric values
        pct_changes = {}
        for key in recent_metrics:
            if key in week_ago_metrics and key != 'Timestamp_EST':
                try:
                    recent_val = float(recent_metrics[key])
                    week_ago_val = float(week_ago_metrics[key])
                    
                    if week_ago_val != 0:
                        pct_change = ((recent_val - week_ago_val) / abs(week_ago_val)) * 100
                        pct_changes[f"{key}_pct_change"] = pct_change
                except (ValueError, TypeError):
                    # Skip non-numeric values
                    pass
        
        # Combine into a single result
        result = {
            'recent': recent_metrics,
            'week_ago': week_ago_metrics,
            'pct_change': pct_changes
        }
        
        return result
    
    def get_date_range_data(
        self, 
        start_date: Union[str, datetime], 
        end_date: Union[str, datetime] = None
    ) -> pd.DataFrame:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
            
        if end_date is None:
            end_date = datetime.now().date()
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()
            
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        query = f"""
        WITH DailyCloseData AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE(Timestamp__UTC)
                    ORDER BY ABS(
                        EXTRACT(HOUR FROM Timestamp__UTC) * 3600 +
                        EXTRACT(MINUTE FROM Timestamp__UTC) * 60 +
                        EXTRACT(SECOND FROM Timestamp__UTC) -
                        {self.market_close_hour_utc} * 3600
                    ) ASC
                ) AS RowRank
            FROM {self.table_name}
            WHERE DATE(Timestamp__UTC) BETWEEN '{start_date_str}' AND '{end_date_str}'
        )
        SELECT *
        FROM DailyCloseData
        WHERE RowRank = 1
        ORDER BY Timestamp__UTC;
        """
        
        return self._execute_query(query)
    
    def get_metric_series(
        self, 
        metric_name: str,
        start_date: Union[str, datetime] = None, 
        end_date: Union[str, datetime] = None
    ) -> pd.Series:
        """Get a time series for any specific metric"""
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).date()
            
        df = self.get_date_range_data(start_date, end_date)
        
        if df.empty:
            return pd.Series()
            
        if metric_name not in df.columns:
            self.logger.error(f"Metric '{metric_name}' not found in data. Available columns: {df.columns.tolist()}")
            return pd.Series()
            
        dates = pd.to_datetime(df['Timestamp__UTC']).dt.tz_convert('US/Eastern')
        metric_values = df[metric_name]
        
        return pd.Series(metric_values.values, index=dates)
    
    def get_risk_metrics(
        self, 
        date: Union[str, datetime, None] = None
    ) -> dict:
        if date is None:
            df, _ = self.get_latest_market_close()
        else:
            df = self.get_market_close_data(date)
            
        if df.empty:
            return {}
            
        # Filter out non-metric columns
        df = df.drop(columns=['RowRank'], errors='ignore')
        
        # Get key metrics using the helper function
        return self._identify_key_metrics(df)