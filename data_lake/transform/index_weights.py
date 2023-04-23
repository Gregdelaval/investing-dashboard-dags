import pandas
import numpy
from .base_transformer import BaseTransformer
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, FLOAT, DATETIME
from sqlalchemy import Column


class IndexCountryWeights(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.sql_types = (
			Column('country', VARCHAR(50), nullable=False),
			Column('common_index_name', VARCHAR(50), nullable=False),
			Column('instrument_name', VARCHAR(50), nullable=False),
			Column('weight_percentage', FLOAT(32, unsigned=True), nullable=False),
			Column('id', VARCHAR(100), primary_key=True),
		)
		self.pandas_types = {
			'country': pandas.StringDtype(storage='pyarrow'),
			'common_index_name': pandas.StringDtype(storage='pyarrow'),
			'instrument_name': pandas.StringDtype(storage='pyarrow'),
			'weight_percentage': numpy.float32,
			'id': pandas.StringDtype(storage='pyarrow'),
		}

	@BaseTransformer.log_call
	def transform_country_weights(
		self,
		df: pandas.DataFrame,
		common_index_name: str,
		instrument_name: str,
	) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(df)
		df['common_index_name'] = common_index_name
		df['instrument_name'] = instrument_name
		df['id'] = df['instrument_name'] + df['country']
		df['weight_percentage'] = df['weight_percentage'].str.replace('%', '')
		df = df.astype(self.pandas_types)
		return df


class IndexSectorWeights(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.sql_types = (
			Column('sector', VARCHAR(50), primary_key=True),
			Column('common_index_name', VARCHAR(50), nullable=False),
			Column('instrument_name', VARCHAR(50), nullable=False),
			Column('weight_percentage', FLOAT(32, unsigned=True), nullable=False),
			Column('id', VARCHAR(100), primary_key=True),
		)
		self.pandas_types = {
			'sector': pandas.StringDtype(storage='pyarrow'),
			'weight_percentage': numpy.float32,
			'common_index_name': pandas.StringDtype(storage='pyarrow'),
			'instrument_name': pandas.StringDtype(storage='pyarrow'),
			'id': pandas.StringDtype(storage='pyarrow'),
		}

	@BaseTransformer.log_call
	def transform_sector_weights(
		self,
		df: pandas.DataFrame,
		common_index_name: str,
		instrument_name: str,
	) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(df)
		df['common_index_name'] = common_index_name
		df['instrument_name'] = instrument_name
		df['id'] = df['instrument_name'] + df['sector']
		df['weight_percentage'] = df['weight_percentage'].str.replace('%', '')
		df = df.astype(self.pandas_types)
		return df


class IndexConstituentsWeights(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.sql_types = (
			Column('asset', VARCHAR(50), nullable=True),
			Column('name', VARCHAR(255), nullable=False),
			Column('common_index_name', VARCHAR(50), nullable=False),
			Column('instrument_name', VARCHAR(50), nullable=False),
			Column('isin', VARCHAR(50), nullable=True),
			Column('cusip', VARCHAR(50), nullable=True),
			Column('shares_number', BIGINT(unsigned=False), nullable=True),
			Column('weight_percentage', FLOAT(32, unsigned=False), nullable=False),
			Column('market_value', FLOAT(32, unsigned=False), nullable=True),
			Column('updated', DATETIME, nullable=True),
			Column('id', VARCHAR(100), primary_key=True),
		)
		self.pandas_types = {
			'asset': pandas.StringDtype(storage='pyarrow'),
			'name': pandas.StringDtype(storage='pyarrow'),
			'common_index_name': pandas.StringDtype(storage='pyarrow'),
			'instrument_name': pandas.StringDtype(storage='pyarrow'),
			'isin': pandas.StringDtype(storage='pyarrow'),
			'cusip': pandas.StringDtype(storage='pyarrow'),
			'shares_number': numpy.int64,
			'weight_percentage': numpy.float32,
			'market_value': numpy.float32,
			'updated': numpy.datetime64,
			'id': pandas.StringDtype(storage='pyarrow'),
		}

	@BaseTransformer.log_call
	def transform_constituents_weights(
		self,
		df: pandas.DataFrame,
		common_index_name: str,
		instrument_name: str,
	) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(df)
		df['common_index_name'] = common_index_name
		df['instrument_name'] = instrument_name
		df['id'] = df['instrument_name'] + df['name'] + df['cusip']

		df = df.astype(self.pandas_types)

		df.dropna(
			subset=[
			'asset',
			'weight_percentage',
			],
			inplace=True,
		)

		df.drop(df[df['weight_percentage'] == 0].index, inplace=True)

		return df
