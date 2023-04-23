import pandas
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.exc import NoSuchTableError
import logging
from typing import List, Callable, Union, Dict, Tuple, Literal
from functools import wraps
import time
from pandas._typing import IndexLabel, DtypeArg


class BaseHelper():

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


class MysqlConnector(BaseHelper):

	def __init__(self, connection_uri: str) -> None:
		"""Helper class for interaction with the MySql database.

		Args:
			connection_uri (str): URI to use for connecting with mysql database.
		"""
		super().__init__(__name__)
		self.connection = create_engine(url=connection_uri.replace('mysql', 'mysql+pymysql'))

	@BaseHelper.log_call
	def write_df_to_sql_database(
		self,
		df: pandas.DataFrame,
		name: str,
		schema: str,
		data_types: Union[Tuple[Column], None] = None,
		if_table_exists: Literal['replace', 'append'] = "replace",
		index: bool = False,
		index_label: IndexLabel = None,
		chunksize: Union[int, None] = None,
		method: Union[str, None] = 'multi',
		attempts: int = 3,
		cooldown: int = 5,
	) -> None:
		"""Wrapper for the df.DataFrame.to_sql function that adds a retrying mechanism.
		Also embeds sqlalchemy Table and MetaData features for free definition of the
		meta data for the table.

		Args:
			df (pandas.DataFrame):
			Df to write to the mysql data base.

			name (str):
			Table name.

			schema (str):
			Schema that the table belongs to.

			data_types (Union[Tuple[Column], None], optional): Defaults to None.
			Accepts a tuple with sqlalchemy.Column
			Example:
			(
				sqlalchemy.Column('date', DATE, nullable=False),
				sqlalchemy.Column('revenue', BIGINT(unsigned=False), nullable=False),
			)

			if_table_exists (Literal['replace', 'append'], optional): Defaults to "replace".
            How to behave if the table already exists.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.

			index (bool, optional): Defaults to False.
			Write DataFrame index as a column. Uses `index_label` as the column
            name in the table.

			index_label (IndexLabel, optional): Defaults to None.
			Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
            A sequence should be given if the DataFrame uses MultiIndex.

			chunksize (Union[int, None], optional): Defaults to None.
			Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once.

			method (Union[str, None], optional): Defaults to multi.
			{None, 'multi', callable}
            Controls the SQL insertion clause used:
            * None : Uses standard SQL ``INSERT`` clause (one per row).
            * 'multi': Pass multiple values in a single ``INSERT`` clause.
            * callable with signature ``(pd_table, conn, keys, data_iter)``.
            Details and a sample callable implementation can be found in the
            section :ref:`insert method <io.sql.method>`.

			attempts (int, optional): Defaults to 3.
			Amount of attempts to successfully write the table.

			cooldown (int, optional): Defaults to 5.
			Cooldown between attempts in seconds.
		"""

		_metadata = MetaData()
		dtype = None
		for attempt in range(attempts):

			if if_table_exists == 'replace':
				try:
					self.connection.execute(f'DROP TABLE IF EXISTS `{schema}`.`{name}`')
					self.log.info(f'Successfully removed table {schema}.{name}')
				except Exception as e:
					self.log.warning(f'Failed deleting {schema}.{name} due to:\n{repr(e)}')
					time.sleep(cooldown)
					continue

			if isinstance(data_types, tuple):
				try:
					Table(
						name,
						_metadata,
						*data_types,
						schema=schema,
					)
					dtype = {_tuple.key: _tuple.type for _tuple in data_types}
					self.log.info(f'Successfully defined meta data for {schema}.{name}.')
				except Exception as e:
					self.log.warning(f'Failed defining meta data for {schema}.{name} due to:\n{repr(e)}')
					time.sleep(cooldown)
					continue

				try:
					_metadata.create_all(self.connection, checkfirst=True)
					self.log.info(f'Successfully created {schema}.{name} or it already exists.')
				except Exception as e:
					self.log.warning(f'Failed creating {schema}.{name} due to:\n{repr(e)}')
					time.sleep(cooldown)
					continue

			try:
				df.to_sql(
					con=self.connection,
					name=name,
					schema=schema,
					if_exists='append',
					index=index,
					index_label=index_label,
					dtype=dtype,
					chunksize=chunksize,
					method=method,
				)
				self.log.info(f'Successfully written df to {schema}.{name}.')
				return
			except Exception as e:
				self.log.warning(f'Exception when writing df to {schema}.{name}:\n {repr(e)}')
				time.sleep(cooldown)
				continue

		raise Exception(f'Failed when writing df to {schema}.{name}')

	@BaseHelper.log_call
	def read_sql_query(
		self,
		sql: str,
		index_col: Union[str, List[str], None] = None,
		coerce_float: bool = True,
		params: Union[List[str], Dict[str, str], Tuple[str, str], None] = None,
		parse_dates: Union[List[str], Dict[str, str], None] = None,
		chunksize: Union[int, None] = None,
		dtype: Union[Dict[str, DtypeArg], DtypeArg, None] = None,
		empty_resultset_policy: Literal['raise', 'return_empty', None] = 'raise',
		attempts: int = 3,
		cooldown: int = 5,
	) -> Union[pandas.DataFrame, None]:
		"""
		Convenice wrapper around the pandas.read_sql_query function,
		added retrying mechanism and flexible policy for handling empty resultset.

		Args:
			sql (str):
			Query to use.

			index_col (Union[str, List[str], None], optional): Defaults to None.
			Column(s) to set as index(MultiIndex).

			coerce_float (bool, optional): Defaults to True.
			Attempts to convert values of non-string, non-numeric objects (like
        	decimal.Decimal) to floating point, useful for SQL result sets.

			params (Union[List[str], Dict[str, str], Tuple[str, str], None], optional): Defaults to None.
			List of parameters to pass to execute method.  The syntax used
			to pass parameters is database driver dependent. Check your
			database driver documentation for which of the five syntax styles,
			described in PEP 249's paramstyle, is supported.
			Eg. for psycopg2, uses %(name)s so use params={'name' : 'value'}.

			parse_dates (Union[List[str], Dict[str, str], None], optional): Defaults to None.
			- List of column names to parse as dates.
			- Dict of ``{column_name: format string}`` where format string is
			strftime compatible in case of parsing string times, or is one of
			(D, s, ns, ms, us) in case of parsing integer timestamps.
			- Dict of ``{column_name: arg dict}``, where the arg dict corresponds
			to the keyword arguments of :func:`pandas.to_datetime`
			Especially useful with databases without native Datetime support,such as SQLite.

			chunksize (Union[int, None], optional): Defaults to None.
			If specified, return an iterator where `chunksize` is the
        	number of rows to include in each chunk.

			dtype (Union[Dict[str, DtypeArg], DtypeArg, None], optional): Defaults to None.
			Data type for data or columns. E.g. np.float64 or
			{'a': np.float64, 'b': np.int32, 'c': 'Int64'}.

			empty_resultset_policy (Literal['raise', 'return_empty', None], optional): Defaults to 'raise'.
			Policy for handling empty query results.
			- 'return_empty': returns an empty pandas.DataFrame
			- 'raise': raises custom EmptyResultsetException
			- None: returns None

			attempts (int, optional): Defaults to 3.
			Amount of attempts to successfully read the table.

			cooldown (int, optional): Defaults to 5.
			Cooldown between attempts in seconds.

		Returns:
			Union[pandas.DataFrame, None]:
			Dependent on empty_resultset_policy value returns either a pandas.DataFrame or None

		"""
		for attempt in range(attempts):
			try:
				df = pandas.read_sql_query(
					sql=sql,
					con=self.connection,
					index_col=index_col,
					coerce_float=coerce_float,
					params=params,
					parse_dates=parse_dates,
					chunksize=chunksize,
					dtype=dtype,
				)
				self.log.info(f'Successfully executed query:\n{sql}')
			except NoSuchTableError as e:
				self.log.warning(f'A table in query doesnt exist. Treating as empty resultset.')
				df = pandas.DataFrame()
			except Exception as e:
				self.log.warning(f'Failed executing query:\n{sql}\n Failed due to:\n{repr(e)}')
				time.sleep(cooldown)
				continue

			if df.empty:
				if empty_resultset_policy == 'raise':
					raise EmptyResultsetException(message=f'Following query returned an empty table:\n{sql}')

				if empty_resultset_policy == 'return_empty':
					return df

				if empty_resultset_policy == None:
					return

			return df

		raise Exception(f'Failed executing query when fetching data.')

	@BaseHelper.log_call
	def read_sql_table(
		self,
		schema: str = None,
		table_name: str = None,
		index_col: Union[str, List[str], None] = None,
		coerce_float: bool = True,
		parse_dates: Union[List[str], Dict[str, str], None] = None,
		columns: Union[List[str], None] = None,
		chunksize: Union[int, None] = None,
		dtype: Union[Dict[str, DtypeArg], DtypeArg, None] = None,
		empty_resultset_policy: Literal['raise', 'return_empty', None] = 'raise',
		attempts: int = 3,
		cooldown: int = 5,
	) -> Union[pandas.DataFrame, None]:
		"""
		Convenice wrapper around the pandas.read_sql_query function,
		added retrying mechanism and flexible policy for handling empty resultset.

		Args:
			schema (str):
			Schema that the table belongs to.

			table_name (str):
			Name of table to read.

			index_col (Union[str, List[str], None], optional): Defaults to None.
			Column(s) to set as index(MultiIndex).

			coerce_float (bool, optional): Defaults to True.
			Attempts to convert values of non-string, non-numeric objects (like
        	decimal.Decimal) to floating point, useful for SQL result sets.

			parse_dates (Union[List[str], Dict[str, str], None], optional): Defaults to None.
			- List of column names to parse as dates.
			- Dict of ``{column_name: format string}`` where format string is
			strftime compatible in case of parsing string times, or is one of
			(D, s, ns, ms, us) in case of parsing integer timestamps.
			- Dict of ``{column_name: arg dict}``, where the arg dict corresponds
			to the keyword arguments of :func:`pandas.to_datetime`
			Especially useful with databases without native Datetime support,such as SQLite.

			columns (Union[List[str], None], optional): Defaults to None.
        	List of column names to select from SQL table.

			chunksize (Union[int, None], optional): Defaults to None.
			If specified, return an iterator where `chunksize` is the
        	number of rows to include in each chunk.

			dtype (Union[Dict[str, DtypeArg], DtypeArg, None], optional): Defaults to None.
			Data type for data or columns. E.g. np.float64 or
			{'a': np.float64, 'b': np.int32, 'c': 'Int64'}.

			empty_resultset_policy (Literal['raise', 'return_empty', None], optional): Defaults to 'raise'.
			Policy for handling empty query results.
			- 'return_empty': returns an empty pandas.DataFrame
			- 'raise': raises custom EmptyResultsetException
			- None: returns None

			attempts (int, optional): Defaults to 3.
			Amount of attempts to successfully read the table.

			cooldown (int, optional): Defaults to 5.
			Cooldown between attempts in seconds.

		Returns:
			Union[pandas.DataFrame, None]:
			Dependent on empty_resultset_policy value returns either a pandas.DataFrame or None
		"""

		for attempt in range(attempts):
			try:
				df = pandas.read_sql_table(
					con=self.connection,
					schema=schema,
					table_name=table_name,
					columns=columns,
					index_col=index_col,
					coerce_float=coerce_float,
					parse_dates=parse_dates,
					chunksize=chunksize,
				)
				self.log.info(f'Successfully read {schema}.{table_name}.')
			except ValueError as e:
				self.log.warning(
					f'Querying {schema}.{table_name} raises:\n{repr(e)}\nTreating as empty result set!'
				)
				df = pandas.DataFrame()
			except NoSuchTableError as e:
				self.log.warning(f'{schema}.{table_name} doesnt exist. Treating as empty resultset.')
				df = pandas.DataFrame()
			except Exception as e:
				self.log.warning(f'Failed reading {schema}.{table_name} due to:\n{repr(e)}')
				time.sleep(cooldown)
				continue

			if df.empty:
				if empty_resultset_policy == 'raise':
					raise EmptyResultsetException(message=f'{schema}.{table_name} returns an empty result set.')

				if empty_resultset_policy == 'return_empty':
					return df

				if empty_resultset_policy == None:
					return

			if dtype is not None:
				try:
					df = df.astype(dtype)
				except Exception as e:
					self.log.warning(f'Failed applying dtype to fetched df due to:\n{repr(e)}')
					continue

			return df

		raise Exception(f'Failed executing query when fetching data.')

	@BaseHelper.log_call
	def remove_tables(
		self,
		*tables: str,
		attempts: int = 2,
		cooldown: int = 5,
	) -> None:
		"""Drops the list of passed schema.tables

		Args:
			*tables (str):
			Schema.table strings to drop,
			like: 'schema.table'

			attempts (int, optional): Defaults to 2.
			Amount of attempts to successfully drop the tables.

			cooldown (int, optional): Defaults to 5.
			Cooldown between attempts in seconds.
		"""

		_query = 'DROP TABLE IF EXISTS ' + \
    ' '.join([f'`{_table.replace(".", r"`.`")}`,' for _table in tables])[:-1]

		for attempt in range(attempts):
			try:
				self.connection.execute(_query)
				self.log.info('Tables successfully removed')
				return
			except Exception as e:
				self.log.warning(f'Failed removing following tables due to:\n{repr(e)}')
				time.sleep(cooldown)
				continue

		raise Exception('Failed removing tables')

	@BaseHelper.log_call
	def replace_table(
		self,
		table_to_replace: str,
		table_to_replace_with: str,
		attempts: int = 3,
		cooldown: int = 5,
	) -> None:
		"""Replaces the sql table `table_to_replace` by `table_to_replace_with`,
		by first removing `table_to_replace` and then renaming `table_to_replace_with`.

		Args:
			table_to_replace (str):
			'{schema}.{table}' to replace.

			table_to_replace_with (str):
			'{schema}.{table}' to replace with.

			attempts (int, optional): Defaults to 2.
			Amount of attempts to successfully drop the tables.

			cooldown (int, optional): Defaults to 5.
			Cooldown between attempts in seconds.

		Raises:
			Exception: Raises base exception if fails to replace.
		"""
		for attempt in range(attempts):
			try:
				self.connection.execute(f'DROP TABLE IF EXISTS `{table_to_replace.replace(".", r"`.`")}`')
				self.log.info(f'{table_to_replace} successfully removed')
			except Exception as e:
				self.log.warning(f'Failed removing {table_to_replace} due to:\n{repr(e)}')
				time.sleep(cooldown)
				continue

			try:
				self.connection.execute(f'RENAME TABLE {table_to_replace_with} TO {table_to_replace}')
				self.log.info(f'{table_to_replace} successfully replaced by {table_to_replace_with}')
				return
			except Exception as e:
				self.log.warning(
					f'Failed replacing {table_to_replace} by {table_to_replace_with} due to:\n{repr(e)}'
				)
				time.sleep(cooldown)
				continue

		raise Exception(f'Failed replacing {table_to_replace} with {table_to_replace_with}')


class EmptyResultsetException(Exception):

	def __init__(self, message: str) -> None:
		super(EmptyResultsetException, self).__init__(message)
