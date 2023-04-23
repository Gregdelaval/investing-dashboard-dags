import pandas
from .base_transformer import BaseTransformer
import numpy
from sqlalchemy.dialects.mysql import BIGINT, FLOAT, DATETIME, CHAR
from sqlalchemy import Column


class InvestingInstruments(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.pandas_types = {
			'datetime': pandas.DatetimeTZDtype(tz='UTC'),
			'currency': pandas.StringDtype(storage='pyarrow'),
			'open': numpy.float32,
			'high': numpy.float32,
			'low': numpy.float32,
			'close': numpy.float32,
			'volume': pandas.Int64Dtype(),
		}
		self.sql_types = (
			Column('datetime', DATETIME(0), primary_key=True),
			Column('open', FLOAT(32, unsigned=True), nullable=False),
			Column('high', FLOAT(32, unsigned=True), nullable=False),
			Column('low', FLOAT(32, unsigned=True), nullable=False),
			Column('close', FLOAT(32, unsigned=True), nullable=False),
			Column('volume', BIGINT(unsigned=True), nullable=False),
			Column('currency', CHAR(3), nullable=True),
		)

	@BaseTransformer.log_call
	def transform_table(
		self,
		df: pandas.DataFrame,
	) -> pandas.DataFrame:

		df.fillna(
			{
			'volume': 0,
			},
			inplace=True,
		)

		df.dropna(
			subset=[
			'close',
			'high',
			'low',
			'open',
			'volume',
			],
			inplace=True,
		)

		df['datetime'] = pandas.to_datetime(
			df['timestamp'],
			unit='s',
		)

		df.drop(axis=1, labels=['timestamp'], inplace=True)

		df = df.astype(self.pandas_types)
		return df
