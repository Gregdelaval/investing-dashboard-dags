import pandas
from .base_transformer import BaseTransformer
import numpy
from sqlalchemy.dialects.mysql import CHAR, VARCHAR, BIGINT, DATE, FLOAT
from sqlalchemy import Column


class EarningsCalendar(BaseTransformer):
	sql_types = (
		Column('date', DATE, nullable=False),
		Column('eps', FLOAT(16), nullable=True),
		Column('eps_estimated', FLOAT(16), nullable=True),
		Column('fiscal_date_ending', DATE, nullable=True),
		Column('id', VARCHAR(50), primary_key=True, nullable=False),
		Column('revenue', BIGINT(), nullable=False),
		Column('revenue_estimated', BIGINT(unsigned=False), nullable=False),
		Column('symbol', VARCHAR(50), nullable=False),
		Column('time', CHAR(3), nullable=False),
		Column('updated_from_date', DATE, nullable=True),
	)

	pandas_types = {
		'date': numpy.datetime64,
		'eps': numpy.float16,
		'eps_estimated': numpy.float16,
		'fiscal_date_ending': numpy.datetime64,
		'id': pandas.StringDtype(storage='pyarrow'),
		'revenue': numpy.int64,
		'revenue_estimated': numpy.int64,
		'symbol': pandas.StringDtype(storage='pyarrow'),
		'time': pandas.StringDtype(storage='pyarrow'),
		'updated_from_date': numpy.datetime64,
	}

	def __init__(self) -> None:
		super().__init__(__name__)

	@BaseTransformer.log_call
	def transform_earnings_calendar(self, df: pandas.DataFrame) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(df)

		df.fillna(
			{
			'revenue': 0,
			'revenue_estimated': 0,
			},
			inplace=True,
		)

		df.dropna(
			subset=[
			'date',
			'symbol',
			],
			inplace=True,
		)

		df['id'] = df['symbol'] + df['date']
		df.drop_duplicates(subset=['id'], keep='last', inplace=True)
		print(df.head(10))
		df = df.astype(self.pandas_types)
		return df