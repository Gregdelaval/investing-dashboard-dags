from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
	tags=['scrape_ops', 'scrape_ninja'],
	schedule_interval=None,
	start_date=datetime(2000, 1, 1),
	catchup=False,
	max_active_tasks=1,
	max_active_runs=1,
	dagrun_timeout=timedelta(seconds=600),
	params={
	#---INPUT---#
	'etoro_user_name': 'gregdelaval',
	'etoro_authorization_key': None,
	#---OUTPUT LOCATIONS---#
	#SCHEMA's
	'dl': 'dl_portfolio',
	'pre_dl': 'pre_dl_portfolio',
	#TABLES
	'prod_orders_table': 'etoro_open_orders',
	'raw_orders_table': 'raw_etoro_open_orders',
	},
)
def etoro_open_orders():
	from helpers.helpers import MysqlConnector
	from data_lake.extract.web_scrapers import WebScrapers
	from airflow.hooks.base import BaseHook
	from airflow.models import Variable

	MysqlConnector = MysqlConnector(
		connection_uri=BaseHook.get_connection('mysql_connection').get_uri()
	)
	WebScrapers = WebScrapers(
		scrape_ninja_api_key=Variable.get('scrape_ninja_api_key'),
		scrape_ops_api_key=Variable.get('scrape_ops_api_key'),
	)

	@task
	def extract_etoro_open_orders():
		params = get_current_context()['params']
		assert params['etoro_authorization_key'] is not None, 'Need to pass a valid etoro auth key!'

		raw_snapshot_df = WebScrapers.extract_etoro_open_orders(
			etoro_authorization_key=params['etoro_authorization_key'],
		)

		MysqlConnector.write_df_to_sql_database(
			df=raw_snapshot_df,
			schema=params['pre_dl'],
			name=params['raw_orders_table'],
			if_table_exists='replace',
		)

	@task
	def transform_etoro_open_orders():
		from data_lake.transform.etoro import EtoroOrders

		params = get_current_context()['params']

		raw_df = MysqlConnector.read_sql_table(
			schema=params['pre_dl'],
			table_name=params['raw_orders_table'],
		)

		transformed_df = EtoroOrders().transform_open_orders(df=raw_df)

		MysqlConnector.write_df_to_sql_database(
			df=transformed_df,
			schema=params['dl'],
			name=params['prod_orders_table'],
			if_table_exists='replace',
			data_types=EtoroOrders().sql_types
		)

		MysqlConnector.remove_tables(f"{params['pre_dl']}.{params['raw_orders_table']}")

	extract_etoro_open_orders() >> transform_etoro_open_orders()


dag = etoro_open_orders()