import logging
import requests
import time
from functools import wraps
from typing import Union, Callable, Dict
import pytz
from datetime import datetime
from dateutil.parser import parse


class BaseExtractor:

	def __init__(
		self,
		logger_name: str,
		scrape_ninja_api_key: str = None,
		scrape_ops_api_key: str = None,
	) -> None:
		"""BaseExtractor class containing set of useful functions for other extractor classes.
		Should not be instantiated and used on its own.

		Args:
			logger_name (str): Used for setting class logger.

			scrape_ninja_api_key (str, optional): Defaults to None.
			Used for passing cloudflare protection if needed.

			scrape_ops_api_key (str, optional): Defaults to None.
			Used for fetching random headers, if needed.
		"""

		self.scrape_ninja_api_key = scrape_ninja_api_key
		self.scrape_ops_api_key = scrape_ops_api_key

		logging.basicConfig(
			level=logging.DEBUG, format='%(asctime)s - %(name)s - %(lineno)d -  %(message)s'
		)
		self.log = logging.getLogger(logger_name)

	def log_call(func: Callable):
		"""
		Decorator for wrapping function with logging of called function name, arguments and execution time.
		"""

		@wraps(func)
		def wrapper(self, *args, **kwargs):
			self.log.info(f"Calling function: {func.__name__}. Args:\n{list(args)} {dict(kwargs)}")
			_start = time.time()
			_response = func(self, *args, **kwargs)
			self.log.info(
				f"Function {func.__name__} finished. Execution time: {round(time.time()-_start, 2)}"
			)
			return _response

		return wrapper

	@log_call
	def send_request(
		self,
		method: str,
		url: str,
		cloudflare_circumvention: bool = False,
		attempts: int = 3,
		cooldown_between_requests: int = 1,
		**kwargs,
	) -> Union[requests.Response, None]:
		"""Wrapper for requests.request function that adds a retrying mechanism.
		Returns either a healthy response object (response.status_code == 2xx) or raises exception.

		Args:
			method (str):
			Method for the new :class:`Request` object: ``GET``, ``OPTIONS``, ``HEAD``, ``POST``, ``PUT``, ``PATCH``, or ``DELETE``.

			url (str):
			URL for the new :class:`Request` object.

			cloudflare_circumvention (bool, optional): Defaults to False.
			If request receiver uses cloudflare protection against scraping,
			route the request through scrape ninja api to circumvent protection.

			attempts (int, optional): Defaults to 3.
			Number of attempts to receive a healthy response.

			cooldown_between_requests (int, optional):  Defaults to 1.
			Cooldown between requests in seconds.

			**kwargs: kwargs get passed to the requests.request function.

		Raises:
			Exception: on failure to fetch healthy data, raises exception with passed URL.

		Returns:
			Union[requests.Response, None]:
			Returns a requests.Response object or None if didn't receive healthy response.
		"""

		if cloudflare_circumvention:
			kwargs['json'] = {
				'url': url,
				'headers': [f"{k}: {v}" for k, v in kwargs['headers'].items()]
			}
			kwargs['headers'] = {
				"Content-Type": "application/json",
				"x-rapidapi-host": "scrapeninja.p.rapidapi.com",
				"x-rapidapi-key": self.scrape_ninja_api_key,
			}
			url = 'https://scrapeninja.p.rapidapi.com/scrape'

			self.log.warning(
				f'''Routing request via scrapeninja api, overridden args:
				url={url}
				headers={kwargs["headers"]}
				json={kwargs["json"]}'''
			)

		for attempt in range(attempts):
			self.log.info(f'Sending {method} request to {url}')
			try:
				response = requests.request(method, url, **kwargs)
				response.raise_for_status()
				self.log.info(f'Received healthy response from {url}')
				return response
			except Exception as e:
				self.log.warning(f'Exception when fetching data from {url}:\n {repr(e)}')

			if attempt != attempts - 1:
				time.sleep(cooldown_between_requests)

		raise Exception(f'Failed fetching data from {url}')

	@log_call
	def convert_datetime_to_timestamp(
		self,
		date_time: Union[str, datetime],
		return_as_int: bool = True,
	) -> datetime.timestamp:
		"""Converts passed datetime to timestamp

		Args:
			date_time (Union[str, datetime]): Accepts only string or datetime objects
			return_as_int (bool, optional): Defaults to True.

		Returns:
			datetime.timestamp
		"""
		if isinstance(date_time, str):
			date_time = parse(date_time)
		if date_time.tzinfo == None:
			date_time = date_time.replace(tzinfo=pytz.UTC)
		date_time_timestamp = date_time.timestamp()
		if return_as_int:
			date_time_timestamp = int(date_time_timestamp)
		return date_time_timestamp

	@log_call
	def get_random_req_headers(
		self,
		override_headers: Union[Dict[str, str], None] = None,
	) -> dict:
		"""Fetches a random set of headers from scrapeops.

		Args:
			override_headers (Union[Dict[str, str], None], optional): Defaults to None.
			Dictionary containing key, value pairs used for overriding returned equivalents
			by the scrape ops api.

		Returns:
			dict: Random set of headers.
		"""

		response = self.send_request(
			method='get',
			url=f'http://headers.scrapeops.io/v1/browser-headers?api_key={self.scrape_ops_api_key}',
			params={
			'api_key': self.scrape_ops_api_key,
			'num_headers': '1',
			},
		)
		headers = response.json()['result'][0]

		if override_headers:
			headers.update(override_headers)

		return headers
