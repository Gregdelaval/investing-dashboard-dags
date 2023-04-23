from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
	tags=['scrape_ops'],
	schedule_interval='0 05 * * *',
	start_date=datetime(2000, 1, 1),
	catchup=False,
	max_active_tasks=1,
	max_active_runs=1,
	dagrun_timeout=timedelta(seconds=7200),
	default_args={
	'retry_delay': timedelta(seconds=30),
	'retries': 1,
	},
	params={
	#---INPUT---#
	'granularities': [
	'1mo',
	'1wk',
	'1d',
	'1h',
	'5m',
	],
	#---DEPENDENCIES---#
	'symbols_mapping_schema': 'dl_supplied_tables',
	'symbols_mapping_table': 'symbols_mapping',
	#---OUTPUT LOCATIONS---#
	#SCHEMAS
	'dl': 'dl_investing_instruments',
	'pre_dl': 'pre_dl_investing_instruments',
	#TABLES
	'prod_instrument_table': '{instrument}_{granularity}',
	'rc_instrument_table': 'rc_{instrument}_{granularity}',
	'transformed_instrument_table': 'transformed_{instrument}_{granularity}',
	'raw_instrument_table': 'raw_{instrument}_{granularity}',
	},
)
def investing_instruments():
	from airflow.hooks.base import BaseHook
	from helpers.helpers import MysqlConnector

	MysqlConnector = MysqlConnector(
		connection_uri=BaseHook.get_connection('mysql_connection').get_uri()
	)

	@task
	def fetch_dependencies(**kwargs):
		params = get_current_context()['params']

		symbols_mapping = MysqlConnector.read_sql_table(
			schema=params['symbols_mapping_schema'],
			table_name=params['symbols_mapping_table'],
			columns=['common_name', 'yahoo_name'],
		)

		#Drop entries where there is no symbol at yahoo to extract
		symbols_mapping.drop(symbols_mapping[symbols_mapping['yahoo_name'] == ''].index, inplace=True)

		kwargs['ti'].xcom_push('symbols_mapping', symbols_mapping.to_dict('records'))

	@task
	def extract_investing_instruments(**kwargs):
		from data_lake.extract.web_scrapers import WebScrapers
		from airflow.models import Variable

		params = get_current_context()['params']
		symbols_mapping = kwargs['ti'].xcom_pull(
			key='symbols_mapping',
			task_ids='fetch_dependencies',
		)

		WebScrapers = WebScrapers(scrape_ops_api_key=Variable.get('scrape_ops_api_key'))

		for instrument in symbols_mapping:
			for granularity in params['granularities']:

				ohlc = WebScrapers.fetch_ohlc_yahoo(
					symbol=instrument['yahoo_name'],
					granularity=granularity,
					period_end=datetime.now(),
					period_start='1970-01-02',
				)

				MysqlConnector.write_df_to_sql_database(
					df=ohlc,
					schema=params['pre_dl'],
					name=params['raw_instrument_table'].format(
					instrument=instrument['common_name'],
					granularity=granularity,
					),
					if_table_exists='replace',
				)

	@task
	def transform_investing_instruments(**kwargs):
		from data_lake.transform.investing_instruments import InvestingInstruments

		params = get_current_context()['params']
		symbols_mapping = kwargs['ti'].xcom_pull(
			key='symbols_mapping',
			task_ids='fetch_dependencies',
		)

		for instrument in symbols_mapping:
			for granularity in params['granularities']:

				raw_df = MysqlConnector.read_sql_table(
					schema=params['pre_dl'],
					table_name=params['raw_instrument_table'].format(
					instrument=instrument['common_name'],
					granularity=granularity,
					),
				)

				transformed_df = InvestingInstruments().transform_table(df=raw_df)

				MysqlConnector.write_df_to_sql_database(
					df=transformed_df,
					schema=params['pre_dl'],
					name=params['transformed_instrument_table'].format(
					instrument=instrument['common_name'],
					granularity=granularity,
					),
					if_table_exists='replace',
					data_types=InvestingInstruments().sql_types,
				)

				MysqlConnector.remove_tables(
					f"{params['pre_dl']}.{params['raw_instrument_table'].format(instrument=instrument['common_name'],granularity=granularity)}"
				)

	@task
	def apply_investing_instruments(**kwargs):
		from data_lake.transform.investing_instruments import InvestingInstruments

		params = get_current_context()['params']
		symbols_mapping = kwargs['ti'].xcom_pull(
			key='symbols_mapping',
			task_ids='fetch_dependencies',
		)

		for instrument in symbols_mapping:
			for granularity in params['granularities']:

				#Declare table names
				transformed_table_name = params['transformed_instrument_table'].format(
					instrument=instrument['common_name'], granularity=granularity
				)
				rc_table_name = params['rc_instrument_table'].format(
					instrument=instrument['common_name'], granularity=granularity
				)
				prod_table_name = params['prod_instrument_table'].format(
					instrument=instrument['common_name'], granularity=granularity
				)

				#Read current prod and new increment
				transformed_df = MysqlConnector.read_sql_table(
					schema=params['pre_dl'],
					table_name=transformed_table_name,
					dtype=InvestingInstruments().pandas_types
				)

				prod_df = MysqlConnector.read_sql_table(
					schema=params['dl'],
					table_name=prod_table_name,
					dtype=InvestingInstruments().pandas_types,
					empty_resultset_policy='return_empty',
				)

				#Prep candidate
				prod_df.drop(prod_df.tail(1).index, inplace=True)
				rc_df = InvestingInstruments().concat_deduplicate(
					base_df=prod_df,
					new_df=transformed_df,
					keep='new',
					subset='datetime',
				)

				#Write candidate
				MysqlConnector.write_df_to_sql_database(
					df=rc_df,
					schema=params['pre_dl'],
					name=rc_table_name,
					data_types=InvestingInstruments().sql_types,
					if_table_exists='replace',
					chunksize=5000,
				)

				#Promote candidate
				MysqlConnector.replace_table(
					table_to_replace=f"{params['dl']}.{prod_table_name}",
					table_to_replace_with=f"{params['pre_dl']}.{rc_table_name}",
				)

				#Clean up
				MysqlConnector.remove_tables(f"{params['pre_dl']}.{transformed_table_name}",)

	fetch_dependencies() >> extract_investing_instruments() >> transform_investing_instruments(
	) >> apply_investing_instruments()


dag = investing_instruments()