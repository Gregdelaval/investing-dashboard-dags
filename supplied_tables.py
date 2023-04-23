from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
	tags=['google_sheets'],
	schedule_interval='0 05 * * *',
	start_date=datetime(2000, 1, 1),
	catchup=False,
	max_active_tasks=1,
	max_active_runs=1,
	dagrun_timeout=timedelta(seconds=600),
	default_args={
	'retry_delay': timedelta(seconds=30),
	'retries': 0,
	},
	params={
	#---INPUT---#
	'google_spread_sheet': 'Supplied Tables',
	#---OUTPUT LOCATIONS---#
	#SCHEMAS
	'dl': 'dl_supplied_tables',
	},
)
def supplied_tables():
	from airflow.hooks.base import BaseHook
	from helpers.helpers import MysqlConnector

	MysqlConnector = MysqlConnector(
		connection_uri=BaseHook.get_connection('mysql_connection').get_uri()
	)

	@task
	def fetch_supplied_google_sheets():
		'''
		Fetches reference table from google sheets.
		Derives input for further extraction from it & passes it with xcom.
		'''
		from data_lake.extract.google_sheets_extractor import GoogleSheetsExtractor
		from airflow.models import Variable

		#Parse context params
		params = get_current_context()['params']

		#Instantiate GoogleSheetsExtractor
		GoogleSheetsExtractor = GoogleSheetsExtractor(client_secret=Variable.get('google_client_key'))

		#List sheets
		sheet_names = GoogleSheetsExtractor.list_sheets_in_spreadsheet(
			spread_sheet_name=params['google_spread_sheet']
		)

		#Extract sheets
		for sheet_name in sheet_names:
			df = GoogleSheetsExtractor.get_records_from_sheet(
				sheet_name=sheet_name,
				spread_sheet_name=params['google_spread_sheet'],
			)

			MysqlConnector.write_df_to_sql_database(
				df=df,
				schema=params['dl'],
				name=sheet_name,
			)

	fetch_supplied_google_sheets()


dag = supplied_tables()