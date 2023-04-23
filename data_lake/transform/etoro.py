import pandas
from .base_transformer import BaseTransformer
import numpy
from sqlalchemy.dialects.mysql import CHAR, VARCHAR, BIGINT, FLOAT, SMALLINT, DATETIME, MEDIUMINT, TINYINT, BOOLEAN
from sqlalchemy import Column
from typing import Tuple, Dict
from pandas._typing import DtypeArg


class EtoroSymbols(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.pandas_types = {
			'exchange_id': numpy.uint8,
			'has_expiration_date': numpy.uint8,
			'instrument_display_name': pandas.StringDtype(storage='pyarrow'),  # x bytes
			'instrument_id': numpy.uint32,
			'instrument_type_id': numpy.uint32,
			'instrument_type_sub_category_id': numpy.float32,
			'is_internal_instrument': numpy.uint8,
			'price_source': pandas.StringDtype(storage='pyarrow'),  # 3-7 bytes
			'symbol_full': pandas.StringDtype(storage='pyarrow'),  # x bytes
			'stocks_industry_id': numpy.float16,
		}

		self.sql_types = (
			Column('exchange_id', TINYINT(unsigned=True), nullable=True),
			Column('has_expiration_date', TINYINT(unsigned=True), nullable=True),
			Column('instrument_display_name', VARCHAR(255), nullable=False),
			Column('instrument_id', MEDIUMINT(unsigned=True), primary_key=True),
			Column('instrument_type_id', MEDIUMINT(unsigned=True), nullable=True),
			Column('instrument_type_sub_category_id', FLOAT(32), nullable=True),
			Column('is_internal_instrument', TINYINT(unsigned=True), nullable=True),
			Column('price_source', VARCHAR(64), nullable=True),
			Column('symbol_full', VARCHAR(255), nullable=False),
			Column('stocks_industry_id', FLOAT(16), nullable=True),
		)

	@BaseTransformer.log_call
	def transform_symbols_mapping(self, df: pandas.DataFrame) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(
			df=df,
			columns_start_with_capital=True,
			treat_as_word=['ID'],
		)

		df.dropna(
			subset=[
			'instrument_id',
			'symbol_full',
			'instrument_display_name',
			],
			inplace=True,
		)

		df = df.astype(self.pandas_types)

		return df


class EtoroHistory(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.pandas_types = {
			'cid': pandas.StringDtype(storage='pyarrow'),  # 8 bytes
			'close_date_time': numpy.datetime64,
			'close_rate': numpy.float64,
			'close_reason': numpy.uint8,
			'instrument_id': numpy.uint32,
			'is_buy': numpy.uint8,
			'leverage': numpy.uint8,
			'mirror_id': numpy.int64,
			'net_profit': numpy.float64,
			'open_date_time': numpy.datetime64,
			'open_rate': numpy.float64,
			'parent_cid': pandas.StringDtype(storage='pyarrow'),  #1-7 bytes
			'parent_position_id': numpy.uint8,
			'position_id': numpy.uint64,
		}

		self.sql_types = (
			Column('cid', CHAR(8), nullable=True),
			Column('close_date_time', DATETIME(0), nullable=True),
			Column('close_rate', FLOAT(32), nullable=True),
			Column('close_reason', SMALLINT(unsigned=True), nullable=True),
			Column('instrument_id', MEDIUMINT(unsigned=True), nullable=False),
			Column('is_buy', TINYINT(unsigned=True), nullable=False),
			Column('leverage', TINYINT(unsigned=True), nullable=False),
			Column('mirror_id', BIGINT(unsigned=False), nullable=True),
			Column('net_profit', FLOAT(32), nullable=False),
			Column('open_date_time', DATETIME(0), nullable=False),
			Column('open_rate', FLOAT(32), nullable=False),
			Column('parent_cid', VARCHAR(64), nullable=True),
			Column('parent_position_id', TINYINT(unsigned=True), nullable=True),
			Column('position_id', BIGINT(unsigned=True), primary_key=True),
		)

	@BaseTransformer.log_call
	def transform_portfolio_history(self, df: pandas.DataFrame) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(
			df=df,
			columns_start_with_capital=True,
			treat_as_word=['ID', 'CID'],
		)

		df = df.astype(self.pandas_types)
		return df


class EtoroOverview(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)

		self.pandas_types = {
			'direction': pandas.StringDtype(storage='pyarrow'),  #3-4 bytes
			'instrument_id': numpy.uint32,
			'invested': numpy.float64,
			'mirror_id': numpy.int64,
			'net_profit': numpy.float64,
			'parent_cid': pandas.StringDtype(storage='pyarrow'),  #1-7 bytes
			'parent_username': pandas.StringDtype(storage='pyarrow'),  #1-7 bytes
			'pending_for_closure': numpy.uint8,
			'realized_credit': numpy.float64,
			'unrealized_credit': numpy.float64,
			'value': numpy.float64,
		}

		self.sql_types = (
			Column('direction', CHAR(4), nullable=False),
			Column('instrument_id', MEDIUMINT(unsigned=True), primary_key=True),
			Column('invested', FLOAT(32), nullable=False),
			Column('mirror_id', BIGINT(unsigned=False), primary_key=True),
			Column('net_profit', FLOAT(32), nullable=False),
			Column('parent_cid', VARCHAR(64), nullable=True),
			Column('parent_username', VARCHAR(64), nullable=True),
			Column('pending_for_closure', TINYINT(unsigned=True), nullable=True),
			Column('realized_credit', FLOAT(32), nullable=True),
			Column('unrealized_credit', FLOAT(32), nullable=True),
			Column('value', FLOAT(32), nullable=False),
		)

	def fetch_sql_types(self, df: pandas.DataFrame) -> Tuple[Column]:
		"""Fetches SQL types for columns in passed DF

		Args:
			df (pandas.DataFrame): DF to fetch applicable sql types for.

		Returns:
			Tuple[Column]: Tuple containing Columns with SQL meta data.
		"""

		return tuple(_tuple for _tuple in self.sql_types if _tuple.key in df.columns.values)

	def fetch_pandas_types(self, df: pandas.DataFrame) -> Dict[str, DtypeArg]:
		"""Fetches pandas types for columns in passed DF

		Args:
			df (pandas.DataFrame): DF to fetch pandas applicable types for.

		Returns:
			Dict[str, DtypeArg]: Mapping column names to pandas dtypes
		"""

		return {k: v for k, v in self.pandas_types.items() if k in df.columns.values}

	@BaseTransformer.log_call
	def transform_snapshot(
		self,
		df: pandas.DataFrame,
		columns_start_with_capital: bool,
	) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(
			df=df,
			columns_start_with_capital=columns_start_with_capital,
			treat_as_word=['ID', 'CID'],
		)

		df.astype(self.fetch_pandas_types(df))

		return df


class EtoroPositions(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.pandas_types = {
			'amount': numpy.float64,
			'cid': pandas.StringDtype(storage='pyarrow'),  # 8 bytes
			'current_rate': numpy.float64,
			'instrument_id': numpy.uint32,
			'is_buy': numpy.uint8,
			'is_tsl_enabled': numpy.uint8,
			'leverage': numpy.uint8,
			'mirror_id': numpy.int64,
			'net_profit': numpy.float64,
			'open_date_time': pandas.DatetimeTZDtype(tz='UTC'),
			'open_rate': numpy.float64,
			'parent_position_id': numpy.uint8,
			'pip_difference': numpy.float64,
			'position_id': numpy.uint64,
			'stop_loss_rate': numpy.float64,
			'take_profit_rate': numpy.float64,
		}

		self.sql_types = (
			Column('amount', FLOAT(32, unsigned=True), nullable=False),
			Column('cid', CHAR(8), nullable=False),
			Column('current_rate', FLOAT(32, unsigned=True), nullable=False),
			Column('instrument_id', MEDIUMINT(unsigned=True), nullable=False),
			Column('is_buy', TINYINT(unsigned=True), nullable=False),
			Column('is_tsl_enabled', TINYINT(unsigned=True), nullable=True),
			Column('leverage', TINYINT(unsigned=True), nullable=False),
			Column('mirror_id', BIGINT(unsigned=False), nullable=True),
			Column('net_profit', FLOAT(32), nullable=False),
			Column('open_date_time', DATETIME(0), nullable=False),
			Column('open_rate', FLOAT(32, unsigned=True), nullable=False),
			Column('parent_position_id', TINYINT(unsigned=True), nullable=True),
			Column('pip_difference', FLOAT(32), nullable=True),
			Column('position_id', BIGINT(unsigned=True), primary_key=True),
			Column('stop_loss_rate', FLOAT(32, unsigned=True), nullable=True),
			Column('take_profit_rate', FLOAT(32, unsigned=True), nullable=True),
		)

	@BaseTransformer.log_call
	def transform_open_positions(self, df: pandas.DataFrame) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(
			df,
			columns_start_with_capital=True,
			treat_as_word=['ID', 'CID'],
		)

		df.astype(self.pandas_types)

		df['open_date_time'] = pandas.to_datetime(df['open_date_time'], unit='ns')

		return df


class EtoroOrders(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.pandas_types = {
			'amount': numpy.float64,
			'cid': pandas.StringDtype(storage='pyarrow'),  # 8 bytes
			'execution_type': numpy.uint8,
			'instrument_id': numpy.uint32,
			'is_buy': numpy.bool_,
			'is_discounted': numpy.bool_,
			'is_tsl_enabled': numpy.bool_,
			'leverage': numpy.uint8,
			'open_date_time': pandas.DatetimeTZDtype(tz='UTC'),
			'order_id': numpy.uint64,
			'rate': numpy.float64,
			'stop_loss_rate': numpy.float64,
			'take_profit_rate': numpy.float64,
			'units': numpy.float64,
		}

		self.sql_types = (
			Column('amount', FLOAT(32, unsigned=True), nullable=False),
			Column('cid', CHAR(8), nullable=False),
			Column('execution_type', TINYINT(unsigned=True), nullable=False),
			Column('instrument_id', MEDIUMINT(unsigned=True), nullable=False),
			Column('is_buy', BOOLEAN, nullable=False),
			Column('is_discounted', BOOLEAN, nullable=False),
			Column('is_tsl_enabled', BOOLEAN, nullable=False),
			Column('leverage', TINYINT(unsigned=True), nullable=False),
			Column('open_date_time', DATETIME(0), nullable=False),
			Column('order_id', BIGINT(unsigned=True), primary_key=True),
			Column('rate', FLOAT(32, unsigned=True), nullable=False),
			Column('stop_loss_rate', FLOAT(32, unsigned=True), nullable=False),
			Column('take_profit_rate', FLOAT(32, unsigned=True), nullable=False),
			Column('units', FLOAT(32, unsigned=True), nullable=False),
		)

	@BaseTransformer.log_call
	def transform_open_orders(self, df: pandas.DataFrame) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(
			df,
			columns_start_with_capital=True,
			treat_as_word=['ID', 'CID'],
		)

		df.astype(self.pandas_types)

		df['open_date_time'] = pandas.to_datetime(df['open_date_time'], unit='ns')

		return df