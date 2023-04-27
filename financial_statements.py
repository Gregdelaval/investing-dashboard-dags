from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
	tags=['FMP'],
	schedule_interval='30 07 * * *',
	start_date=datetime(2000, 1, 1),
	catchup=False,
	max_active_tasks=1,
	max_active_runs=1,
	dagrun_timeout=timedelta(seconds=7200),
	default_args={
	'retries': 0,
	'retry_delay': timedelta(seconds=60),
	},
	params={
	#---INPUT---#
	#By default extracts recently updated financials.
	#Can be overwritten by setting 'extract_only_following_symbols' to a list of symbols to extract.
	'extract_only_following_symbols': [],
	#---DEPENDENCIES---#
	'calendar_schema': 'dl_company_information',
	'calendar_table': 'earnings_calendar',
	'index_information_schema': 'dl_index_information',
	'constituents_table': 'consolidated_constituents_weights',
	#---OUTPUT LOCATIONS---#
	#SCHEMAS
	'dl': 'dl_company_information',
	'pre_dl': 'pre_dl_company_information',
	#TABLES
	'prod_table': '{granularity}_{financial_statement}',
	'rc_table': 'rc_{granularity}_{financial_statement}',
	'transformed_table': 'transformed_{granularity}_{financial_statement}',
	'raw_table': 'raw_{granularity}_{financial_statement}',
	},
)
def financial_statements():
	from helpers.helpers import MysqlConnector
	from airflow.hooks.base import BaseHook

	MysqlConnector = MysqlConnector(
		connection_uri=BaseHook.get_connection('mysql_connection').get_uri()
	)

	@task.short_circuit
	def fetch_and_define_dependencies(**kwargs):
		from helpers.helpers import EmptyResultsetException
		import pendulum

		context = get_current_context()

		if len(context['params']['extract_only_following_symbols']):
			symbols_to_extract = context['params']['extract_only_following_symbols']
		else:
			#Define symbols to extract data for
			try:
				symbols_to_extract = MysqlConnector.read_sql_query(
					empty_resultset_policy='raise',
					sql='''
					WITH earnings_calendar AS (
						SELECT `date`, `symbol`
						FROM `{calendar_schema}`.`{calendar_table}`
					), constituents AS (
						SELECT DISTINCT `asset`
						FROM `{index_information_schema}`.`{constituents_table}`
					)
					SELECT
						c.`asset`
					FROM earnings_calendar ec
					JOIN constituents c
					ON c.`asset` = ec.`symbol`
					WHERE ec.`date` BETWEEN DATE('{start_date}') AND DATE('{end_date}')
					'''.format(
					calendar_schema=context['params']['calendar_schema'],
					calendar_table=context['params']['calendar_table'],
					index_information_schema=context['params']['index_information_schema'],
					constituents_table=context['params']['constituents_table'],
					start_date=pendulum.parse(context['ts']).subtract(days=3).strftime('%Y-%m-%d'),
					end_date=pendulum.parse(context['ts']).subtract(days=1).strftime('%Y-%m-%d'),
					),
				)['asset'].tolist()
			except EmptyResultsetException:  #Stop pipeline if there are no new financials to extract
				return False

		kwargs['ti'].xcom_push('symbols_to_extract', symbols_to_extract)
		return True

	@task
	def extract_financials(**kwargs):
		from airflow.models import Variable
		from data_lake.extract.fmp_extractor import FMPExtractor

		params = get_current_context()['params']
		symbols_to_extract = kwargs['ti'].xcom_pull(
			key='symbols_to_extract',
			task_ids='fetch_and_define_dependencies',
		)

		FMPExtractor = FMPExtractor(Variable.get('fmp_api_key'))

		for financial_statement in ['income_statement', 'balance_sheet_statement', 'cash_flow_statement']:
			for granularity in ['annual', 'quarterly']:

				increments_table = params['raw_table'].format(
					financial_statement=financial_statement,
					granularity=granularity,
				)

				MysqlConnector.remove_tables(f"{params['pre_dl']}.{increments_table}")

				for symbol in symbols_to_extract:
					df = FMPExtractor.extract_financial_statement(
						symbol=symbol,
						financial_statement=financial_statement,
						granularity=granularity,
					)

					MysqlConnector.write_df_to_sql_database(
						df=df,
						schema=params['pre_dl'],
						name=increments_table,
						if_table_exists='append',
					)

	@task
	def transform_financials():
		from data_lake.transform.company_financials import CompanyFinancials

		params = get_current_context()['params']

		for financial_statement in ['income_statement', 'balance_sheet_statement', 'cash_flow_statement']:
			for granularity in ['annual', 'quarterly']:

				raw_df = MysqlConnector.read_sql_table(
					schema=params['pre_dl'],
					table_name=params['raw_table'].format(
					financial_statement=financial_statement,
					granularity=granularity,
					),
				)

				transformed_df = CompanyFinancials().transform_financial_statement(df=raw_df)

				MysqlConnector.write_df_to_sql_database(
					df=transformed_df,
					schema=params['pre_dl'],
					name=params['transformed_table'].format(
					financial_statement=financial_statement,
					granularity=granularity,
					),
					if_table_exists='replace',
					data_types=CompanyFinancials().fetch_sql_types(df=transformed_df),
				)

	@task
	def apply_financials():
		from data_lake.transform.company_financials import CompanyFinancials

		params = get_current_context()['params']

		for financial_statement in ['income_statement', 'balance_sheet_statement', 'cash_flow_statement']:
			for granularity in ['annual', 'quarterly']:

				#Declare table names
				raw_table = params['raw_table'].format(
					financial_statement=financial_statement, granularity=granularity
				)
				transformed_table = params['transformed_table'].format(
					financial_statement=financial_statement, granularity=granularity
				)
				rc_table = params['rc_table'].format(
					financial_statement=financial_statement, granularity=granularity
				)
				prod_table = params['prod_table'].format(
					financial_statement=financial_statement, granularity=granularity
				)

				#Apply increment
				transformed_df = MysqlConnector.read_sql_table(
					schema=params['pre_dl'],
					table_name=transformed_table,
				)
				transformed_df = transformed_df.astype(CompanyFinancials().fetch_pandas_types(transformed_df))

				prod_df = MysqlConnector.read_sql_table(
					schema=params['dl'],
					table_name=prod_table,
					empty_resultset_policy='return_empty',
				)
				prod_df = prod_df.astype(CompanyFinancials().fetch_pandas_types(prod_df))

				rc_df = CompanyFinancials().concat_deduplicate(
					base_df=prod_df,
					new_df=transformed_df,
					keep='new',
					subset='id',
				)

				#Write candidate
				sql_types = CompanyFinancials().fetch_sql_types(df=rc_df)
				MysqlConnector.write_df_to_sql_database(
					df=rc_df,
					schema=params['pre_dl'],
					name=rc_table,
					data_types=sql_types,
					if_table_exists='replace',
				)

				#Promote candidate
				MysqlConnector.replace_table(
					table_to_replace=f"{params['dl']}.{prod_table}",
					table_to_replace_with=f"{params['pre_dl']}.{rc_table}",
				)

				#Clean up
				MysqlConnector.remove_tables(
					f"{params['pre_dl']}.{raw_table}",
					f"{params['pre_dl']}.{transformed_table}",
				)

	fetch_and_define_dependencies() >> extract_financials() >> transform_financials(
	) >> apply_financials()


dag = financial_statements()