from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
	tags=['FMP'],
	schedule_interval='15 07 * * *',
	start_date=datetime(2000, 1, 1),
	catchup=False,
	max_active_tasks=1,
	max_active_runs=1,
	dagrun_timeout=timedelta(seconds=1200),
	default_args={
	'retries': 2,
	'retry_delay': timedelta(seconds=30),
	},
	params={
	#---OUTPUT LOCATIONS---#
	#SCHEMAS
	'dl': 'dl_company_information',
	'pre_dl': 'pre_dl_company_information',
	#TABLES
	'calendar_table': 'earnings_calendar',
	'raw_calendar_table': 'raw_earnings_calendar',
	},
)
def earnings_calendar():
	from helpers.helpers import MysqlConnector
	from airflow.hooks.base import BaseHook

	MysqlConnector = MysqlConnector(
		connection_uri=BaseHook.get_connection('mysql_connection').get_uri()
	)

	@task
	def extract_earnings_calendar():
		from airflow.models import Variable
		from data_lake.extract.fmp_extractor import FMPExtractor
		import pendulum

		context = get_current_context()

		FMPExtractor = FMPExtractor(Variable.get('fmp_api_key'))

		raw_calendar = FMPExtractor.extract_earnings_calendar(
			start_date=pendulum.parse(context['ts']).subtract(weeks=1),
			end_date=pendulum.parse(context['ts']).add(weeks=12),
		)

		MysqlConnector.write_df_to_sql_database(
			df=raw_calendar,
			schema=context['params']['pre_dl'],
			name=context['params']['raw_calendar_table'],
			if_table_exists='replace',
		)

	@task
	def transform_earnings_calendar():
		from data_lake.transform.earnings_calendar import EarningsCalendar

		params = get_current_context()['params']

		raw_earnings_calendar = MysqlConnector.read_sql_table(
			schema=params['pre_dl'],
			table_name=params['raw_calendar_table'],
		)

		transformed_earnings_calendar = EarningsCalendar().transform_earnings_calendar(
			df=raw_earnings_calendar,
		)

		MysqlConnector.write_df_to_sql_database(
			df=transformed_earnings_calendar,
			schema=params['dl'],
			name=params['calendar_table'],
			data_types=EarningsCalendar.sql_types,
			if_table_exists='replace',
		)

		MysqlConnector.remove_tables(f"{params['pre_dl']}.{params['raw_calendar_table']}")

	extract_earnings_calendar() >> transform_earnings_calendar()


dag = earnings_calendar()