from .base_extractor import BaseExtractor
import pandas
from typing import Union, Literal
import datetime
from dateutil.parser import parse


class FMPExtractor(BaseExtractor):

	def __init__(self, fmp_api_key: str) -> None:
		super().__init__(__name__)
		self.fmp_api_key = fmp_api_key

	@BaseExtractor.log_call
	def extract_index_country_weights(
		self,
		symbol: str,
	):
		url = f'https://financialmodelingprep.com/api/v3/etf-country-weightings/{symbol}?apikey={self.fmp_api_key}'
		response = self.send_request(url=url, method='get')
		df = pandas.DataFrame.from_records(response.json())
		return df

	@BaseExtractor.log_call
	def extract_index_sector_weights(
		self,
		symbol: str,
	):
		url = f'https://financialmodelingprep.com/api/v3/etf-sector-weightings/{symbol}?apikey={self.fmp_api_key}'
		response = self.send_request(url=url, method='get')
		df = pandas.DataFrame.from_records(response.json())
		return df

	@BaseExtractor.log_call
	def extract_index_constituents_weights(
		self,
		symbol: str,
	):
		url = f'https://financialmodelingprep.com/api/v3/etf-holder/{symbol}?apikey={self.fmp_api_key}'
		response = self.send_request(url=url, method='get')
		df = pandas.DataFrame.from_records(response.json())
		return df

	@BaseExtractor.log_call
	def extract_financial_statement(
		self,
		symbol: str,
		financial_statement: Literal['balance_sheet_statement', 'cash_flow_statement',
		'income_statement'],
		granularity: Literal['annual', 'quarterly'],
	) -> pandas.DataFrame:
		"""Extracts financial statement from FMP.

		Args:
			symbol (str):
			Symbol to extract statement for

			financial_statement (Literal['balance_sheet_statement', 'cash_flow_statement', 'income_statement']):
			Type of financial statement to extract.

			granularity (Literal['annual', 'quarterly']):
			Granularity of statement. Options:

		Returns:
			pandas.DataFrame: DF containing the statement.
		"""

		financial_statement = financial_statement.replace('_', '-')

		if granularity == 'annual':
			granularity = ''
		else:
			granularity = 'period=quarter&'

		url = f'https://financialmodelingprep.com/api/v3/{financial_statement}/{symbol}?{granularity}limit=999&apikey={self.fmp_api_key}'
		response = self.send_request(url=url, method='get')
		df = pandas.DataFrame.from_records(response.json())
		return df

	@BaseExtractor.log_call
	def extract_earnings_calendar(
		self,
		start_date: Union[str, datetime.datetime],
		end_date: Union[str, datetime.datetime],
	) -> pandas.DataFrame:
		"""Fetches earnings calendar from FMP

		Args:
			start_date (Union[str, datetime.datetime]):
			Start date for extraction.

			end_date (Union[str, datetime.datetime]):
			End date for extraction.

		Returns:
			pandas.DataFrame: Returns content as a pandas DF
		"""

		if isinstance(start_date, str):
			start_date = parse(start_date)
		if isinstance(end_date, str):
			end_date = parse(end_date)

		response = self.send_request(
			method='get',
			url=
			f'https://financialmodelingprep.com/api/v3/earning_calendar?from={start_date.strftime("%Y-%m-%d")}&to={end_date.strftime("%Y-%m-%d")}&apikey={self.fmp_api_key}',
		)
		df = pandas.DataFrame.from_records(response.json())
		return df
