import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass
from datetime import datetime
from internal_data.gre.rpc import RPC
#from internal_data.databricks.databricks_query_gre import DatabricksGREQuery
from external_data.coinmetrics.coinmetrics_client import CoinMetricsClient
from models.trader import Trader
from models.position import Position
from utils.config import TRADER_MAPPING, GRE_TRADER_MAPPING
from dotenv import load_dotenv
import os

class GREProcessor:
    def __init__(self, mapping_file_path, data_source="beacon", as_of_date=None):
        """
        Initialize GREProcessor with configurable data source and historical date.
        
        Args:
            mapping_file_path (str): Path to the mapping file
            data_source (str): Either "beacon" or "databricks"
            as_of_date (str, optional): Date in YYYY-MM-DD format for historical processing
        """
        load_dotenv()
        secrets_dir = os.getenv('SECRETS_DIR')
        if not secrets_dir:
            raise ValueError("SECRETS_DIR not found in environment variables")

        self.data_source = data_source
        # Store the provided date or use current date as fallback
        self.as_of_date = as_of_date or datetime.now().strftime('%Y-%m-%d')
        print(f"GREProcessor initialized with as_of_date: {self.as_of_date}")
        
        # Initialize clients based on data source
        if self.data_source == "beacon":
            self.beacon_rpc = RPC(secrets_dir=secrets_dir)
        #elif self.data_source == "databricks":
        #    self.databricks_query = DatabricksGREQuery()
        
        self.coinmetrics_client = CoinMetricsClient()
        self.mapping_file_path = mapping_file_path
        
        # Initialize price data caches
        self.price_data_cache = {}
        self.no_price_data_tickers: Set[str] = set()

    def fetch_data(self, databricks_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Fetch data from either Beacon RPC or Databricks."""
        if self.data_source == "databricks":
            if databricks_df is not None:
                df = databricks_df
            else:
                df = self.databricks_query.get_gre_positions(self.as_of_date)
                
            # Map Databricks columns to Beacon format
            column_mapping = {
                'Pod_L2': 'Pod(L2)',
                'Book_L3': 'Book(L3)',
                'Business_L0': 'Business(L0)',
                'Strategy_L4': 'Strategy(L4)',
                'PositionBlock_L5': 'PositionBlock(L5)',
            }
            
            df = df.rename(columns=column_mapping)
            return df
        
        try:
            # Format date parameter for the API
            rpt_date = None
            if self.as_of_date:
                # Remove any hyphens if they exist
                rpt_date = self.as_of_date.replace('-', '')
                print(f"Using report date: {rpt_date} (from as_of_date: {self.as_of_date})")
            
            # First try with the date parameter
            try:
                print(f"Attempting to call Beacon API with date parameter: {rpt_date}")
                response = self.beacon_rpc.get_rpc('users/galaxy/jc/beacon_api/generate_gre', 
                                                  rpt_date=rpt_date)
            except Exception as e:
                print(f"Failed with date parameter, error: {str(e)}")
                # If that fails, try without the date parameter (fallback to default behavior)
                print("Attempting to call Beacon API without date parameter...")
                response = self.beacon_rpc.get_rpc('users/galaxy/jc/beacon_api/generate_gre')
            res_dict = response.json()
            
            if not res_dict.get('content'):
                raise ValueError("No content received from Beacon RPC")
                
            columns = res_dict['content'][0]
            rows = res_dict['content'][1:]
            
            return pd.DataFrame(rows, columns=columns)
            
        except Exception as e:
            print(f"Error fetching data from Beacon RPC: {str(e)}")
            print(f"API endpoint attempted: 'users/galaxy/jc/beacon_api/generate_gre'")
            print(f"Date parameter used: {rpt_date if 'rpt_date' in locals() else 'None'}")
            raise RuntimeError(f"Failed to fetch data from Beacon RPC: {str(e)}")

    def _map_underlier(self, row) -> str:
        underlier = row['Underlier']

        if underlier in ['IBIT', 'ARKB', 'BTCO', 'GBTC']:
            return 'BTC'
        
        if underlier == 'XAPO':
            return 'BTC'
        
        if underlier == 'BTC_MT_GOX':
            return 'BTC'
        
        if underlier in ['QETH', 'ETHE']:
            return 'ETH'
        
        if underlier == 'FTX_SOL':
            return 'SOL'
        
        if underlier == 'LOCKED_ENA':
            return 'ENA'

        if underlier == 'LOCKED_AVAX':
            return 'AVAX'

        return underlier

    def _get_internal_sub_grouping(self, underlier: str, ticker: str) -> str:
        """Determine the internal sub-grouping based on underlier and ticker."""
        # BTC related instruments
        btc_identifiers = {
            'BTC', 'IBIT', 'FBTC', 'WBTC', 'BTC_MT_GOX', 'ARKB', 'BTCO', 
            'GBTC', 'XAPO'
        }
        
        # ETH related instruments
        eth_identifiers = {'ETH', 'QETH', 'ETHE'}
        
        # SOL related instruments
        sol_identifiers = {'SOL', 'FTX_SOL'}
        
        # Check both underlier and ticker against identifiers
        for identifier in (underlier, ticker):
            if identifier in btc_identifiers:
                return 'BTC'
            if identifier in eth_identifiers:
                return 'ETH'
            if identifier in sol_identifiers:
                return 'SOL'
        
        return 'Alts'

    def _should_include_trader(self, trader_raw: str, ticker: str = None) -> bool:
        """
        Determine if a trader should be included based on GRE_TRADER_MAPPING.
        Special handling for CryptoHedges: only include if ticker is BTC-related.
        """
        if trader_raw == 'CryptoHedges':
            # Only include CryptoHedges positions if they are BTC-related
            btc_identifiers = {
                'BTC', 'IBIT', 'FBTC', 'WBTC', 'BTC_MT_GOX', 'ARKB', 'BTCO', 
                'GBTC', 'XAPO'
            }
            return ticker in btc_identifiers if ticker else False
            
        allowed_traders = {'Novo', 'Beimnet', 'Bouchra', 'Felman'}
        return trader_raw in allowed_traders

    def _preprocess_price_data(self, df: pd.DataFrame):
        """Pre-process all unique underliers to get price data in batch."""
        # Get unique underliers after mapping
        unique_underliers = set()
        for _, row in df.iterrows():
            mapped_underlier = self._map_underlier(row)
            unique_underliers.add(mapped_underlier)

        # Process all underliers through CoinMetrics
        results = self.coinmetrics_client.process_mapping(
            self.mapping_file_path,  # Use the stored mapping file path
            list(unique_underliers),
            end_date=self.as_of_date if self.data_source == "databricks" else None
        )
        
        # Create aligned dataframes
        df_prices, df_returns = self.coinmetrics_client.create_aligned_dfs(results)
        
        # Cache the results
        for underlier in unique_underliers:
            if underlier in df_returns.columns:
                return_series = df_returns[underlier].values
                latest_price = df_prices[underlier].iloc[0] if not df_prices[underlier].empty else None
                self.price_data_cache[underlier] = (return_series, latest_price)
            else:
                self.no_price_data_tickers.add(underlier)

    def _get_price_data(self, underlier: str) -> Optional[Tuple[np.ndarray, float]]:
        """Get cached price data for an underlier."""
        if underlier in self.price_data_cache:
            return self.price_data_cache[underlier]
        self.no_price_data_tickers.add(underlier)
        return None

    def _parse_option_details(self, ticker: str) -> Tuple[Optional[float], Optional[str]]:
        """Parse strike price and expiry from option ticker."""
        if not ticker or '-' not in ticker:
            return None, None
            
        try:
            # Split ticker by '-' and '='
            parts = ticker.split('=')[0].split('-')
            if len(parts) < 4:
                return None, None
                
            # Get strike from the last part before the '='
            strike = float(parts[3])  # Changed from -2 to 3 since format is BTCUSD-2025FEB28-C-110000=GALAXY_HK
            
            # Parse expiry date (format: 2025FEB28)
            expiry_str = parts[1]
            expiry_date = datetime.strptime(expiry_str, '%Y%b%d')
            formatted_expiry = expiry_date.strftime('%Y-%m-%dT00:00:00')
            
            return strike, formatted_expiry
        except (ValueError, IndexError) as e:
            print(f"Error parsing option details from ticker {ticker}: {str(e)}")
            return None, None

    def _create_position(self, row, trader_obj, trader_name):
        """Create a position object regardless of price data availability."""
        if abs(row.get('$Delta', 0)) < 1e-10:
            return None

        # Check if trader should be included, passing both trader and ticker
        trader_raw = row['Pod(L2)']
        ticker = row['Ticker']
        if not self._should_include_trader(trader_raw, ticker):
            return None

        # Initialize all variables before creating Position object
        mapped_underlier = self._map_underlier(row)
        security_type = row['Type']
        strategy = f"{row['Strategy(L4)']} {row['PositionBlock(L5)']}"
        underlier = row['Underlier']
        sid = None
        delta = row['$Delta']
        gamma = row['$Gamma']
        vega = row['$Vega']
        theta = row['$Theta']
        percent_delta = row['%Delta']
        quantity = row['Quantity']
        price = row['Price']
        underlying_price = row['Price']
        contract_size = 1
        market_value = row['Value']

        # Get price data and calculate volatility
        price_data = self._get_price_data(mapped_underlier)
        return_series = np.array([])
        latest_price = None
        volatility = 0.0
        dollar_volatility = 0.0
        
        if price_data is not None:
            return_series, latest_price = price_data
            volatility = np.std(return_series)
            dollar_volatility = volatility * delta

        # Parse strike and expiry for options
        strike = None
        expiry = None
        if security_type == "CryptoOption":
            strike, expiry = self._parse_option_details(ticker)
        else:
            expiry = row['Expiry']
            
        # Calculate notional value
        notional_value = 0
        if security_type == "CryptoOption" and strike is not None:
            notional_value = quantity * strike * contract_size
        else:
            # For non-options, use the underlier price
            notional_value = quantity * price * contract_size

        # Set position attributes with new categorization
        price_source = 'CoinMetrics' if price_data is not None else 'Missing'
        
        # Special handling for CryptoHedges BTC positions
        if trader_raw == 'CryptoHedges':
            internal_grouping = 'Other_Macro'
            internal_sub_grouping = 'BTC'
        else:
            internal_grouping = 'Crypto_Macro'
            internal_sub_grouping = self._get_internal_sub_grouping(underlier, ticker)
            
        amount_risked = delta

        # Create Position object with pre-initialized variables
        position = Position(
            trader=trader_obj,
            trader_name=trader_name,
            strategy=strategy,
            security_type=security_type,
            underlier=underlier,
            ticker=ticker,
            sid=sid,
            market_value=market_value,
            notional_value=notional_value,
            delta=delta,
            gamma=gamma,
            vega=vega,
            theta=theta,
            percent_delta=percent_delta,
            expiry=expiry,
            strike=strike,
            quantity=quantity,
            price_source=price_source,
            price=price,
            underlying_price=underlying_price,
            contract_size=contract_size,
            return_time_series=return_series,
            volatility=volatility,
            dollar_volatility=dollar_volatility,
            amount_risked=amount_risked,
            internal_grouping=internal_grouping,
            internal_sub_grouping=internal_sub_grouping,
            ignore=False
        )

        return position

    def process_data(self, data: pd.DataFrame) -> Tuple[List[Position], List[str]]:
        """Process GRE data into positions."""
        # Pre-process all price data first
        self._preprocess_price_data(data)
        
        processed_positions = []
        for _, row in data.iterrows():
            trader_raw = row['Pod(L2)']
            trader_mapped_name = GRE_TRADER_MAPPING.get(trader_raw, trader_raw)
            
            if row['Book(L3)'] != 'Crypto' or row['Business(L0)'] != 'PrincipalTrading':
                continue

            trader_obj = TRADER_MAPPING.get(trader_mapped_name)
            if trader_obj is None:
                trader_obj = Trader(trader_mapped_name, 'Unknown Group')

            position = self._create_position(row, trader_obj, trader_mapped_name)
            if position is not None:
                processed_positions.append(position)

        return processed_positions, list(self.no_price_data_tickers)

    def process(self, databricks_df: Optional[pd.DataFrame] = None) -> Tuple[List[Position], List[str]]:
        """Main processing pipeline."""
        df = self.fetch_data(databricks_df)
        return self.process_data(df)