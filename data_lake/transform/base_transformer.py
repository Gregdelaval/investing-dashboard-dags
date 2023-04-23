import logging
import pandas
from typing import List, Callable, Union, Literal
import time
from functools import wraps


class BaseTransformer:

	def __init__(self, name) -> None:

		#Define logger
		logging.basicConfig(
			level=logging.DEBUG, format='%(asctime)s - %(name)s - %(lineno)d -  %(message)s'
		)
		self.log = logging.getLogger(name)

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

	def camel_case_to_snake_case_df_columns(
		self,
		df: pandas.DataFrame,
		columns_start_with_capital: bool = False,
		treat_as_word: List[str] = None,
	) -> pandas.DataFrame:
		"""Renames columns in passed df for camelCase to snake_case

		Args:
			df (pandas.DataFrame): DF to rename columns in
			columns_start_with_capital (bool, optional): Defaults to False.
			Whether column names start with a LeadingCapitalLetter

			treat_as_word (List[str], optional): Defaults to None.
			Example:
			treat_as_word = ['ID']
			example column name = 'providedUserID'
			output: 'provided_user_id' (instead of 'provided_user_i_d')

		Returns:
			pandas.DataFrame: DataFrame with new column names
		"""
		columns_list = df.columns.values

		for i, column_name in enumerate(columns_list):

			if isinstance(treat_as_word, list):
				matches = [word for word in treat_as_word if word in column_name]
				if matches:
					longest_match = sorted(matches, key=len, reverse=True)[0]
					column_name = column_name.replace(longest_match, longest_match[0] + longest_match[1:].lower())
					columns_list[i] = column_name

			for letter in column_name:
				if letter.isupper():
					column_name = column_name.replace(letter, f'_{letter.lower()}')
					columns_list[i] = column_name

			if columns_start_with_capital:
				columns_list[i] = column_name[1:]

		df.set_axis(columns_list, axis=1, inplace=True)
		return df

	@log_call
	def concat_deduplicate(
		self,
		base_df: pandas.DataFrame,
		new_df: pandas.DataFrame,
		keep: Literal['new', 'old', None] = 'new',
		subset: Union[str, List[str], None] = None,
	) -> pandas.DataFrame:
		"""
		Concats two dataframes and filters out duplicates.

		Args:
			base_df (pandas.DataFrame):
			DF to apply new increment on top of.

			new_df (pandas.DataFrame):
			DF increment to apply on top of base_df.

			keep (Literal['new', 'old', None], optional): Defaults to 'new'.
			Whether to keep duplicates from the new_df, old_df or neither.

			subset (Union[str, List[str], None], optional): Defaults to None.
			Whether to only evaluate a subset of columns when removing duplicates.

		Returns:
			pandas.DataFrame: Concat and deduplicated DF of passed new_df and base_df
		"""

		_df = pandas.concat([base_df, new_df])

		if keep == 'new':
			keep = 'last'
		elif keep == 'old':
			keep = 'first'

		_df.drop_duplicates(keep=keep, inplace=True, subset=subset)

		return _df
