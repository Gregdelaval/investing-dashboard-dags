from .base_extractor import BaseExtractor
from datetime import datetime
from bs4 import BeautifulSoup, Tag
import pandas
import json
from typing import Union, Literal
import pendulum


class WebScrapers(BaseExtractor):

	def __init__(
		self,
		scrape_ninja_api_key: str = None,
		scrape_ops_api_key: str = None,
	) -> None:
		"""
		Args:
			scrape_ninja_api_key (str, optional): Defaults to None.
			Used for passing cloudflare protection if needed.

			scrape_ops_api_key (str, optional): Defaults to None.
			Used for fetching random headers, if needed.
		"""
		super().__init__(
			logger_name=__name__,
			scrape_ninja_api_key=scrape_ninja_api_key,
			scrape_ops_api_key=scrape_ops_api_key,
		)

	@BaseExtractor.log_call
	def fetch_ohlc_yahoo(
		self,
		symbol: str,
		granularity: str,
		period_start: Union[str, datetime],
		period_end: Union[str, datetime],
	) -> pandas.DataFrame:
		"""Fetches OHLC candles for passed symbol and granularity.
		Automatically adjusts passed period to span allowed by Yahoo.

		Args:
			symbol (str):
			Symbol as defined on yahoo finance website

			granularity (str):
			Accepted values are: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo

			period_start (Union[str, datetime]):
			Start date for extraction

			period_end (Union[str, datetime]):
			End date for extraction

		Returns:
			pandas.DataFrame:
			DF containing: timestamp, timezone, currency, low, high, open, close, volume
		"""

		if isinstance(period_start, str):
			period_start = pendulum.parse(period_start)
		if isinstance(period_end, str):
			period_end = pendulum.parse(period_end)

		#Applying allowed req period limits if necessary
		allowed_days_since_period_start = {
			'1h': 730,
			'90m': 60,
			'60m': 730,
			'30m': 60,
			'15m': 60,
			'5m': 60,
			'2m': 60,
			'1m': 7,
		}
		if granularity in allowed_days_since_period_start.keys():
			days_allowed = allowed_days_since_period_start[granularity]
			if (pendulum.now() - period_start).days > days_allowed:
				period_start = pendulum.now().subtract(days=days_allowed).add(seconds=30)
				self.log.warning(f'Period requested too long. Setting start date within limit: {period_start}')

		period_start_timestamp = self.convert_datetime_to_timestamp(date_time=period_start)
		period_end_timestamp = self.convert_datetime_to_timestamp(date_time=period_end)

		url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?symbol={symbol}&period1={period_start_timestamp}&period2={period_end_timestamp}&interval={granularity}'

		headers = self.get_random_req_headers(override_headers={'authority': 'query1.finance.yahoo.com'})

		response = self.send_request(url=url, method='get', headers=headers)

		_result = json.loads(response.text)['chart']['result'][0]
		_values = _result['indicators']['quote'][0]
		_df = dict(
			timestamp=_result['timestamp'],
			currency=_result['meta']['currency'],
			low=_values['low'],
			high=_values['high'],
			open=_values['open'],
			close=_values['close'],
			volume=_values['volume'],
		)

		return pandas.DataFrame.from_records(_df)

	@BaseExtractor.log_call
	def extract_etoro_open_orders(
		self,
		etoro_authorization_key: str,
	) -> pandas.DataFrame:

		url = 'https://www.etoro.com/api/logindata/v1.1/logindata?'
		headers = self.get_random_req_headers(
			override_headers={
			"authority": "www.etoro.com",
			"accounttype": "Real",
			"applicationidentifier": "ReToro",
			"applicationversion": "504.0.3",
			"authorization": etoro_authorization_key,
			"referer": "https://www.etoro.com/portfolio/overview",
			}
		)

		response = self.send_request(
			method="post",
			url=url,
			headers=headers,
			cloudflare_circumvention=True,
			attempts=1,
		)

		response_body = json.loads(response.json()["body"])
		all_orders = []
		try:
			for order in response_body['AggregatedResult']['ApiResponses']['PrivatePortfolio']['Content'][
				'ClientPortfolio']['Orders']:
				all_orders.append(order)
			return pandas.DataFrame(all_orders)
		except Exception as e:
			self.log.error(
				f'Failed extracting open orders from response due to:\n{repr(e)}\nResponse body:\n{response_body}'
			)

	@BaseExtractor.log_call
	def extract_etoro_portfolio_positions(
		self,
		user: str,
		instrument_id: Union[str, int],
	) -> pandas.DataFrame:

		headers = self.get_random_req_headers(
			override_headers={
			"authority": "www.etoro.com",
			"accounttype": "Real",
			"applicationidentifier": "ReToro",
			"applicationversion": "504.0.3",
			"referer": f"https://www.etoro.com/people/{user}/portfolio",
			}
		)
		url = f"https://www.etoro.com/sapi/trade-data-real/live/public/positions?cid=29728369&InstrumentID={instrument_id}"

		r = self.send_request(
			method='post',
			url=url,
			headers=headers,
			cloudflare_circumvention=True,
			attempts=1,
		)
		response_body = json.loads(r.json()['body'])
		return pandas.DataFrame.from_records(response_body['PublicPositions'])

	@BaseExtractor.log_call
	def extract_etoro_symbols_mapping(self):
		headers = self.get_random_req_headers(override_headers={'authority': 'api.etorostatic.com'})
		params = {
			'bulkNumber': '1',
			'totalBulks': '1',
		}
		response = self.send_request(
			method='get',
			url='https://api.etorostatic.com/sapi/instrumentsmetadata/V1.1/instruments/bulk',
			params=params,
			headers=headers,
		)

		df = pandas.DataFrame.from_dict(json.loads(response.text)['InstrumentDisplayDatas'])
		df.drop(columns=['Images'], inplace=True)

		return df

	@BaseExtractor.log_call
	def extract_recent_etoro_portfolio_history(
		self,
		user: str,
		current_date_time: Union[str, datetime],
	) -> pandas.DataFrame:

		if isinstance(current_date_time, str):
			current_date_time = pendulum.parse(current_date_time)

		start_date = current_date_time.subtract(years=1).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
		url = f"https://www.etoro.com/sapi/trade-data-real/history/public/credit/flat?StartTime={start_date}Z&PageNumber=1&ItemsPerPage=50&PublicHistoryPortfolioFilter=&CID=29728369",
		headers = self.get_random_req_headers(
			override_headers={
			'authority': 'www.etoro.com',
			'accounttype': 'Real',
			'applicationidentifier': 'ReToro',
			'referer': f'https://www.etoro.com/people/{user}/portfolio/history',
			}
		)
		response = self.send_request(
			method='post',
			url=url,
			headers=headers,
			cloudflare_circumvention=True,
			attempts=1,
		)

		response_body = json.loads(response.json()['body'])
		df = pandas.DataFrame.from_records(response_body['PublicHistoryPositions'])
		return df

	@BaseExtractor.log_call
	def extract_etoro_portfolio_overview(self, user: str) -> dict[pandas.DataFrame]:
		url = 'https://www.etoro.com/sapi/trade-data-real/live/public/portfolios?cid=29728369'
		headers = self.get_random_req_headers(
			override_headers={
			'authority': 'www.etoro.com',
			'accounttype': 'Real',
			'applicationidentifier': 'ReToro',
			'referer': f'https://www.etoro.com/people/{user}/portfolio',
			}
		)

		r = self.send_request(
			method='post',
			url=url,
			headers=headers,
			cloudflare_circumvention=True,
			attempts=1,
		)

		response_body = json.loads(r.json()['body'])
		return {
			'credit':
			pandas.DataFrame.from_records({
			'realized_credit': [response_body['CreditByRealizedEquity']],
			'unrealized_credit': [response_body['CreditByUnrealizedEquity']],
			}),
			'aggregated_mirrors':
			pandas.DataFrame.from_records(response_body['AggregatedMirrors']),
			'aggregated_positions':
			pandas.DataFrame.from_records(response_body['AggregatedPositions']),
		}

	@BaseExtractor.log_call
	def extract_slickcharts_weights_table(
		self,
		index: Literal['sp500', 'nasdaq100', 'dowjones'],
	) -> pandas.DataFrame:
		"""Fetches index constituents and their weights from slickcharts.com

		Args:
			index (Literal['sp500', 'nasdaq100', 'dowjones']): Index to fetch constituents for

		Returns:
			pandas.DataFrame: Returns result set as a pandas DF.
		"""
		headers = self.get_random_req_headers(
			override_headers={
			'authority': 'www.slickcharts.com',
			'referer': f'https://www.slickcharts.com/{index}',
			}
		)
		url = f'https://www.slickcharts.com/{index}'
		response = self.send_request(url=url, method='get', headers=headers)

		#Make some soup
		soup = BeautifulSoup(response.text, features='lxml')

		#Extract entries from table
		entries = []
		for _tr in soup.find_all('tr'):
			_entry = {}
			for i, child in enumerate(_tr.children):
				if type(child) == Tag:
					if i == 3:
						_entry['name'] = child.get_text()
					if i == 5:
						_entry['symbol'] = child.get_text()
					if i == 7:
						_entry['weight'] = child.get_text()
			entries.append(_entry)

		#return as pandas df
		return pandas.DataFrame.from_records(entries[1:])