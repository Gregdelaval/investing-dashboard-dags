from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context


@dag(
	tags=['scrape_ops', 'scrape_ninja'],
	schedule_interval='15 06 * * *',
	start_date=datetime(2000, 1, 1),
	catchup=False,
	max_active_tasks=1,
	max_active_runs=1,
	dagrun_timeout=timedelta(seconds=600),
	default_args={
	'retries': 1,
	'retry_delay': timedelta(seconds=60),
	},
	params={
	#---INPUT---#
	'etoro_user_name': 'gregdelaval',
	#---OUTPUT LOCATIONS---#
	#SCHEMA's
	'dl': 'dl_portfolio',
	'pre_dl': 'pre_dl_portfolio',
	#ETORO SYMBOLS MAPPING TABLES
	'prod_symbols_mapping': 'etoro_symbols_mapping',
	'raw_symbols_mapping': 'raw_etoro_symbols_mapping',
	#PORTFOLIO OVERVIEW TABLES
	'prod_aggregated_mirrors_table': 'etoro_aggregated_mirrors',
	'prod_aggregated_positions_table': 'etoro_aggregated_positions',
	'prod_credit_table': 'etoro_credit',
	'raw_aggregated_mirrors_table': 'raw_etoro_aggregated_mirrors',
	'raw_aggregated_positions_table': 'raw_etoro_aggregated_positions',
	'raw_credit_table': 'raw_etoro_credit',
	#PORTFOLIO HISTORY TABLES
	'prod_portfolio_history': 'etoro_portfolio_history',
	'rc_portfolio_history': 'rc_etoro_portfolio_history',
	'transformed_portfolio_history': 'transformed_etoro_portfolio_history',
	'raw_portfolio_history': 'raw_etoro_portfolio_history',
	#PORTFOLIO POSITIONS TABLES
	'prod_positions_table': 'etoro_positions',
	'raw_positions_table': 'raw_etoro_positions',
	},
)
def etoro_portfolio():
	from json import JSONDecodeError
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

	@task_group(prefix_group_id=False)
	def etoro_symbols_mapping():

		@task
		def extract_symbols_mapping():
			params = get_current_context()['params']

			snapshot_df = WebScrapers.extract_etoro_symbols_mapping()

			MysqlConnector.write_df_to_sql_database(
				df=snapshot_df,
				schema=params['pre_dl'],
				name=params['raw_symbols_mapping'],
				if_table_exists='replace',
			)

		@task
		def transform_symbols_mapping():
			from data_lake.transform.etoro import EtoroSymbols

			params = get_current_context()['params']

			raw_df = MysqlConnector.read_sql_table(
				schema=params['pre_dl'],
				table_name=params['raw_symbols_mapping'],
			)

			prod_df = EtoroSymbols().transform_symbols_mapping(df=raw_df)

			MysqlConnector.write_df_to_sql_database(
				df=prod_df,
				schema=params['dl'],
				name=params['prod_symbols_mapping'],
				if_table_exists='replace',
				data_types=EtoroSymbols().sql_types,
			)

			MysqlConnector.remove_tables(f"{params['pre_dl']}.{params['raw_symbols_mapping']}")

		extract_symbols_mapping() >> transform_symbols_mapping()

	@task_group(prefix_group_id=False)
	def portfolio_history():

		@task
		def extract_portfolio_history():
			'Extracts a snapshot of the top 50 latest positions in etoro portfolio history'
			context = get_current_context()

			for i in range(3):
				try:
					snapshot_df = WebScrapers.extract_recent_etoro_portfolio_history(
						user=context['params']['etoro_user_name'],
						current_date_time=context['ts'],
					)
					break
				except JSONDecodeError:
					continue

			MysqlConnector.write_df_to_sql_database(
				df=snapshot_df,
				schema=context['params']['pre_dl'],
				name=context['params']['raw_portfolio_history'],
			)

		@task
		def transform_portfolio_history():
			from data_lake.transform.etoro import EtoroHistory

			params = get_current_context()['params']

			raw_df = MysqlConnector.read_sql_table(
				schema=params['pre_dl'],
				table_name=params['raw_portfolio_history'],
			)

			transformed_df = EtoroHistory().transform_portfolio_history(df=raw_df)

			MysqlConnector.write_df_to_sql_database(
				df=transformed_df,
				schema=params['pre_dl'],
				name=params['transformed_portfolio_history'],
				if_table_exists='replace',
				data_types=EtoroHistory().sql_types,
			)

		@task
		def apply_portfolio_history():
			from data_lake.transform.etoro import EtoroHistory

			params = get_current_context()['params']

			transformed_df = MysqlConnector.read_sql_table(
				schema=params['pre_dl'],
				table_name=params['transformed_portfolio_history'],
				dtype=EtoroHistory().pandas_types,
			)

			prod_df = MysqlConnector.read_sql_table(
				schema=params['dl'],
				table_name=params['prod_portfolio_history'],
				dtype=EtoroHistory().pandas_types,
			)

			rc_df = EtoroHistory().concat_deduplicate(
				base_df=prod_df,
				new_df=transformed_df,
				keep='new',
				subset='position_id',
			)

			MysqlConnector.write_df_to_sql_database(
				df=rc_df,
				schema=params['pre_dl'],
				name=params['rc_portfolio_history'],
				data_types=EtoroHistory().sql_types,
				if_table_exists='replace',
			)

			MysqlConnector.replace_table(
				table_to_replace=f"{params['dl']}.{params['prod_portfolio_history']}",
				table_to_replace_with=f"{params['pre_dl']}.{params['rc_portfolio_history']}",
			)

			MysqlConnector.remove_tables(
				f"{params['pre_dl']}.{params['raw_portfolio_history']}",
				f"{params['pre_dl']}.{params['transformed_portfolio_history']}",
			)

		extract_portfolio_history() >> transform_portfolio_history() >> apply_portfolio_history()

	@task_group(prefix_group_id=False)
	def portfolio_overview():

		@task
		def extract_portfolio_overview():
			params = get_current_context()['params']

			#Get snapshot containing multiple dfs holding different parts of response
			for i in range(3):
				try:
					dfs = WebScrapers.extract_etoro_portfolio_overview(user=params['etoro_user_name'])
					break
				except JSONDecodeError:
					continue

			#Store tables from snapshot
			MysqlConnector.write_df_to_sql_database(
				df=dfs['credit'],
				schema=params['pre_dl'],
				name=params['raw_credit_table'],
			)
			MysqlConnector.write_df_to_sql_database(
				df=dfs['aggregated_mirrors'],
				schema=params['pre_dl'],
				name=params['raw_aggregated_mirrors_table'],
			)
			MysqlConnector.write_df_to_sql_database(
				df=dfs['aggregated_positions'],
				schema=params['pre_dl'],
				name=params['raw_aggregated_positions_table'],
			)

		@task
		def transform_portfolio_overview():
			from data_lake.transform.etoro import EtoroOverview

			params = get_current_context()['params']

			for table in [
				{
				'raw_table_name': params['raw_aggregated_mirrors_table'],
				'prod_table_name': params['prod_aggregated_mirrors_table'],
				'columns_start_with_capital': True,
				},
				{
				'raw_table_name': params['raw_aggregated_positions_table'],
				'prod_table_name': params['prod_aggregated_positions_table'],
				'columns_start_with_capital': True,
				},
				{
				'raw_table_name': params['raw_credit_table'],
				'prod_table_name': params['prod_credit_table'],
				'columns_start_with_capital': False,
				},
			]:

				raw_df = MysqlConnector.read_sql_table(
					schema=params['pre_dl'],
					table_name=table['raw_table_name'],
				)

				prod_df = EtoroOverview().transform_snapshot(
					df=raw_df,
					columns_start_with_capital=table['columns_start_with_capital'],
				)

				MysqlConnector.write_df_to_sql_database(
					df=prod_df,
					schema=params['dl'],
					name=table['prod_table_name'],
					if_table_exists='replace',
					data_types=EtoroOverview().fetch_sql_types(prod_df),
				)

				MysqlConnector.remove_tables(f"{params['pre_dl']}.{table['raw_table_name']}")

		extract_portfolio_overview() >> transform_portfolio_overview()

	@task_group(prefix_group_id=False)
	def portfolio_positions():

		@task.short_circuit
		def fetch_open_positions_dependencies(**kwargs):
			from helpers.helpers import EmptyResultsetException

			params = get_current_context()['params']

			#Define instrument_ids to extract open positions for
			try:
				instrument_ids = MysqlConnector.read_sql_query(
					empty_resultset_policy='raise',
					sql=f'''
					SELECT DISTINCT(instrument_id)
					FROM `{params["dl"]}`.`{params["prod_aggregated_positions_table"]}`''',
				)['instrument_id'].tolist()
			except EmptyResultsetException:  #Stop pipeline if there are no instrument_ids
				return False

			kwargs['ti'].xcom_push('instrument_ids', instrument_ids)
			return True

		@task
		def extract_open_positions(**kwargs):
			params = get_current_context()['params']

			instrument_ids = kwargs['ti'].xcom_pull(
				key='instrument_ids',
				task_ids='fetch_open_positions_dependencies',
			)

			MysqlConnector.remove_tables(f"{params['pre_dl']}.{params['raw_positions_table']}")
			for instrument_id in instrument_ids:

				for i in range(3):
					try:
						instrument_positions = WebScrapers.extract_etoro_portfolio_positions(
							instrument_id=instrument_id,
							user=params['etoro_user_name'],
						)
						break
					except JSONDecodeError:
						continue

				MysqlConnector.write_df_to_sql_database(
					df=instrument_positions,
					schema=params['pre_dl'],
					name=params['raw_positions_table'],
					if_table_exists='append',
				)

		@task
		def transform_open_positions():
			from data_lake.transform.etoro import EtoroPositions

			params = get_current_context()['params']

			raw_df = MysqlConnector.read_sql_table(
				schema=params['pre_dl'],
				table_name=params['raw_positions_table'],
			)

			prod_df = EtoroPositions().transform_open_positions(df=raw_df)

			MysqlConnector.write_df_to_sql_database(
				df=prod_df,
				schema=params['dl'],
				name=params['prod_positions_table'],
				if_table_exists='replace',
				data_types=EtoroPositions().sql_types,
			)

			MysqlConnector.remove_tables(f"{params['pre_dl']}.{params['raw_positions_table']}")

		fetch_open_positions_dependencies() >> extract_open_positions() >> transform_open_positions()

	portfolio_overview() >> portfolio_positions()
	portfolio_history()
	etoro_symbols_mapping()


dag = etoro_portfolio()