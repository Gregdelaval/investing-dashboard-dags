from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
	tags=['FMP'],
	schedule_interval='45 05 6 * *',
	start_date=datetime(2000, 1, 1),
	catchup=False,
	max_active_tasks=1,
	max_active_runs=1,
	dagrun_timeout=timedelta(seconds=600),
	default_args={
	'retries': 2,
	'retry_delay': timedelta(seconds=30),
	},
	params={
	#---DEPENDENCIES---#
	'symbols_mapping_schema': 'dl_supplied_tables',
	'symbols_mapping_table': 'symbols_mapping',
	#---OUTPUT LOCATIONS---#
	#SCHEMAS
	'dl': 'dl_index_information',
	'pre_dl': 'pre_dl_index_information',
	#TABLES
	'consolidated_constituents_table': 'consolidated_constituents_weights',
	'transformed_constituents_table': '{index}_constituents_weights',
	'raw_constituents_table': 'raw_{index}_constituents_weights',
	},
)
def index_constituents_weights():
	from helpers.helpers import MysqlConnector, EmptyResultsetException
	from airflow.hooks.base import BaseHook

	MysqlConnector = MysqlConnector(
		connection_uri=BaseHook.get_connection('mysql_connection').get_uri()
	)

	@task
	def fetch_and_define_dependencies(**kwargs):
		context = get_current_context()

		try:
			symbols_to_extract = MysqlConnector.read_sql_table(
				empty_resultset_policy='raise',
				schema=context['params']['symbols_mapping_schema'],
				table_name=context['params']['symbols_mapping_table'],
				columns=['common_name', 'tradeable_etf'],
			)

			#Drop entries where there is no symbol at yahoo to extract
			symbols_to_extract.drop(
				symbols_to_extract[symbols_to_extract['tradeable_etf'] == ''].index, inplace=True
			)

		except EmptyResultsetException:
			return False

		kwargs['ti'].xcom_push('symbols_to_extract', symbols_to_extract.to_dict('records'))
		return True

	@task
	def extract_constituents_weights(**kwargs):
		from data_lake.extract.fmp_extractor import FMPExtractor
		from airflow.models import Variable

		params = get_current_context()['params']
		symbols_to_extract = kwargs['ti'].xcom_pull(
			key='symbols_to_extract',
			task_ids='fetch_and_define_dependencies',
		)

		FMPExtractor = FMPExtractor(Variable.get('fmp_api_key'))

		for symbol in symbols_to_extract:
			df = FMPExtractor.extract_index_constituents_weights(symbol['tradeable_etf'])

			if df.empty:
				continue

			MysqlConnector.write_df_to_sql_database(
				df=df,
				schema=params['pre_dl'],
				name=params['raw_constituents_table'].format(index=symbol['common_name']),
				if_table_exists='replace',
			)

	@task
	def transform_constituents_weights(**kwargs):
		from data_lake.transform.index_weights import IndexConstituentsWeights

		params = get_current_context()['params']
		symbols_to_extract = kwargs['ti'].xcom_pull(
			key='symbols_to_extract',
			task_ids='fetch_and_define_dependencies',
		)

		for symbol in symbols_to_extract:
			try:
				raw_constituents_weights = MysqlConnector.read_sql_table(
					schema=params['pre_dl'],
					table_name=params['raw_constituents_table'].format(index=symbol['common_name']),
					empty_resultset_policy='raise',
				)
			except EmptyResultsetException:
				continue

			transformed_constituents_weights = IndexConstituentsWeights().transform_constituents_weights(
				df=raw_constituents_weights,
				common_index_name=symbol['common_name'],
				instrument_name=symbol['tradeable_etf'],
			)

			MysqlConnector.write_df_to_sql_database(
				df=transformed_constituents_weights,
				schema=params['pre_dl'],
				name=params['transformed_constituents_table'].format(index=symbol['common_name']),
				data_types=IndexConstituentsWeights().sql_types,
			)

			MysqlConnector.remove_tables(
				f"{params['pre_dl']}.{params['raw_constituents_table'].format(index=symbol['common_name'])}"
			)

	@task
	def consolidate_index_constituents_weights(**kwargs):
		from data_lake.transform.index_weights import IndexConstituentsWeights

		params = get_current_context()['params']
		symbols_to_extract = kwargs['ti'].xcom_pull(
			key='symbols_to_extract',
			task_ids='fetch_and_define_dependencies',
		)

		MysqlConnector.remove_tables(f"{params['dl']}.{params['consolidated_constituents_table']}")

		for symbol in symbols_to_extract:
			try:
				transformed_constituents_weights = MysqlConnector.read_sql_table(
					schema=params['pre_dl'],
					table_name=params['transformed_constituents_table'].format(index=symbol['common_name']),
					dtype=IndexConstituentsWeights().pandas_types,
					empty_resultset_policy='raise',
				)
			except EmptyResultsetException:
				continue

			MysqlConnector.write_df_to_sql_database(
				df=transformed_constituents_weights,
				schema=params['dl'],
				name=params['consolidated_constituents_table'],
				if_table_exists='append',
				data_types=IndexConstituentsWeights().sql_types,
			)

			MysqlConnector.remove_tables(
				f"{params['pre_dl']}.{params['transformed_constituents_table'].format(index=symbol['common_name'])}"
			)

	fetch_and_define_dependencies() >> extract_constituents_weights(
	) >> transform_constituents_weights() >> consolidate_index_constituents_weights()


dag = index_constituents_weights()
