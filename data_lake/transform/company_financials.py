import pandas
from .base_transformer import BaseTransformer
import numpy
from sqlalchemy.dialects.mysql import CHAR, VARCHAR, BIGINT, DATE, FLOAT, SMALLINT
from sqlalchemy import Column
from typing import Tuple, Dict
from pandas._typing import DtypeArg


class CompanyFinancials(BaseTransformer):

	def __init__(self) -> None:
		super().__init__(__name__)
		self.pandas_types = {
			'accepted_date': numpy.datetime64,
			'account_payables': numpy.int64,
			'accounts_payables': numpy.int64,
			'accounts_receivables': numpy.int64,
			'accumulated_other_comprehensive_income_loss': numpy.int64,
			'acquisitions_net': numpy.int64,
			'calendar_year': numpy.uint16,
			'capital_expenditure': numpy.int64,
			'capital_lease_obligations': numpy.int64,
			'cash_and_cash_equivalents': numpy.int64,
			'cash_and_short_term_investments': numpy.int64,
			'cash_at_beginning_of_period': numpy.int64,
			'cash_at_end_of_period': numpy.int64,
			'change_in_working_capital': numpy.int64,
			'cik': pandas.StringDtype(storage='pyarrow'),  #10 bits
			'common_stock': numpy.int64,
			'common_stock_issued': numpy.int64,
			'common_stock_repurchased': numpy.int64,
			'cost_and_expenses': numpy.int64,
			'cost_of_revenue': numpy.int64,
			'date': numpy.datetime64,
			'debt_repayment': numpy.int64,
			'deferred_income_tax': numpy.int64,
			'deferred_revenue': numpy.int64,
			'deferred_revenue_non_current': numpy.int64,
			'deferred_tax_liabilities_non_current': numpy.int64,
			'depreciation_and_amortization': numpy.int64,
			'dividends_paid': numpy.int64,
			'ebitda': numpy.int64,
			'ebitdaratio': numpy.float32,
			'effect_of_forex_changes_on_cash': numpy.int64,
			'eps': numpy.float32,
			'epsdiluted': numpy.float32,
			'filling_date': numpy.datetime64,
			'final_link': pandas.StringDtype(storage='pyarrow'),
			'free_cash_flow': numpy.int64,
			'general_and_administrative_expenses': numpy.int64,
			'goodwill': numpy.int64,
			'goodwill_and_intangible_assets': numpy.int64,
			'gross_profit': numpy.int64,
			'gross_profit_ratio': numpy.float32,
			'id': pandas.StringDtype(storage='pyarrow'),  #PK derived from symbol+calendar_year+period
			'income_before_tax': numpy.int64,
			'income_before_tax_ratio': numpy.float32,
			'income_tax_expense': numpy.int64,
			'intangible_assets': numpy.int64,
			'interest_expense': numpy.int64,
			'interest_income': numpy.int64,
			'inventory': numpy.int64,
			'investments_in_property_plant_and_equipment': numpy.int64,
			'link': pandas.StringDtype(storage='pyarrow'),
			'long_term_debt': numpy.int64,
			'long_term_investments': numpy.int64,
			'minority_interest': numpy.int64,
			'net_cash_provided_by_operating_activities': numpy.int64,
			'net_cash_used_for_investing_activites': numpy.int64,
			'net_cash_used_provided_by_financing_activities': numpy.int64,
			'net_change_in_cash': numpy.int64,
			'net_debt': numpy.int64,
			'net_income': numpy.int64,
			'net_income_ratio': numpy.float32,
			'net_receivables': numpy.int64,
			'operating_cash_flow': numpy.int64,
			'operating_expenses': numpy.int64,
			'operating_income': numpy.int64,
			'operating_income_ratio': numpy.float32,
			'other_assets': numpy.int64,
			'other_current_assets': numpy.int64,
			'other_current_liabilities': numpy.int64,
			'other_expenses': numpy.int64,
			'other_financing_activites': numpy.int64,
			'other_investing_activites': numpy.int64,
			'other_liabilities': numpy.int64,
			'other_non_cash_items': numpy.int64,
			'other_non_current_assets': numpy.int64,
			'other_non_current_liabilities': numpy.int64,
			'other_working_capital': numpy.int64,
			'othertotal_stockholders_equity': numpy.int64,
			'period': pandas.StringDtype(storage='pyarrow'),  # 2 bits
			'preferred_stock': numpy.int64,
			'property_plant_equipment_net': numpy.int64,
			'purchases_of_investments': numpy.int64,
			'reported_currency': pandas.StringDtype(storage='pyarrow'),
			'research_and_development_expenses': numpy.int64,
			'retained_earnings': numpy.int64,
			'revenue': numpy.int64,
			'sales_maturities_of_investments': numpy.int64,
			'selling_and_marketing_expenses': numpy.int64,
			'selling_general_and_administrative_expenses': numpy.int64,
			'short_term_debt': numpy.int64,
			'short_term_investments': numpy.int64,
			'stock_based_compensation': numpy.int64,
			'symbol': pandas.StringDtype(storage='pyarrow'),
			'tax_assets': numpy.int64,
			'tax_payables': numpy.int64,
			'total_assets': numpy.int64,
			'total_current_assets': numpy.int64,
			'total_current_liabilities': numpy.int64,
			'total_debt': numpy.int64,
			'total_equity': numpy.int64,
			'total_investments': numpy.int64,
			'total_liabilities': numpy.int64,
			'total_liabilities_and_stockholders_equity': numpy.int64,
			'total_liabilities_and_total_equity': numpy.int64,
			'total_non_current_assets': numpy.int64,
			'total_non_current_liabilities': numpy.int64,
			'total_other_income_expenses_net': numpy.int64,
			'total_stockholders_equity': numpy.int64,
			'weighted_average_shs_out': numpy.int64,
			'weighted_average_shs_out_dil': numpy.int64,
		}

		#Update below so that all columns of type bigint become not nullable
		self.sql_types = (
			Column('accepted_date', DATE, nullable=True),
			Column('account_payables', BIGINT(unsigned=False), nullable=False),
			Column('accounts_payables', BIGINT(unsigned=False), nullable=False),
			Column('accounts_receivables', BIGINT(unsigned=False), nullable=False),
			Column('accumulated_other_comprehensive_income_loss', BIGINT(unsigned=False), nullable=False),
			Column('acquisitions_net', BIGINT(unsigned=False), nullable=False),
			Column('calendar_year', SMALLINT(unsigned=True), nullable=False),
			Column('capital_expenditure', BIGINT(unsigned=False), nullable=False),
			Column('capital_lease_obligations', BIGINT(unsigned=False), nullable=False),
			Column('cash_and_cash_equivalents', BIGINT(unsigned=False), nullable=False),
			Column('cash_and_short_term_investments', BIGINT(unsigned=False), nullable=False),
			Column('cash_at_beginning_of_period', BIGINT(unsigned=False), nullable=False),
			Column('cash_at_end_of_period', BIGINT(unsigned=False), nullable=False),
			Column('change_in_working_capital', BIGINT(unsigned=False), nullable=False),
			Column('cik', CHAR(10), nullable=True),
			Column('common_stock', BIGINT(unsigned=False), nullable=False),
			Column('common_stock_issued', BIGINT(unsigned=False), nullable=False),
			Column('common_stock_repurchased', BIGINT(unsigned=False), nullable=False),
			Column('cost_and_expenses', BIGINT(unsigned=False), nullable=False),
			Column('cost_of_revenue', BIGINT(unsigned=False), nullable=False),
			Column('date', DATE, nullable=False),
			Column('debt_repayment', BIGINT(unsigned=False), nullable=False),
			Column('deferred_income_tax', BIGINT(unsigned=False), nullable=False),
			Column('deferred_revenue', BIGINT(unsigned=False), nullable=False),
			Column('deferred_revenue_non_current', BIGINT(unsigned=False), nullable=False),
			Column('deferred_tax_liabilities_non_current', BIGINT(unsigned=False), nullable=False),
			Column('depreciation_and_amortization', BIGINT(unsigned=False), nullable=False),
			Column('dividends_paid', BIGINT(unsigned=False), nullable=False),
			Column('ebitda', BIGINT(unsigned=False), nullable=False),
			Column('ebitdaratio', FLOAT(32), nullable=True),
			Column('effect_of_forex_changes_on_cash', BIGINT(unsigned=False), nullable=False),
			Column('eps', FLOAT(32), nullable=True),
			Column('epsdiluted', FLOAT(32), nullable=True),
			Column('filling_date', DATE, nullable=True),
			Column('final_link', VARCHAR(255), nullable=True),
			Column('free_cash_flow', BIGINT(unsigned=False), nullable=False),
			Column('general_and_administrative_expenses', BIGINT(unsigned=False), nullable=False),
			Column('goodwill', BIGINT(unsigned=False), nullable=False),
			Column('goodwill_and_intangible_assets', BIGINT(unsigned=False), nullable=False),
			Column('gross_profit', BIGINT(unsigned=False), nullable=False),
			Column('gross_profit_ratio', FLOAT(32), nullable=True),
			Column('income_before_tax', BIGINT(unsigned=False), nullable=False),
			Column('id', VARCHAR(50), primary_key=True, nullable=False),
			Column('income_before_tax_ratio', FLOAT(32), nullable=True),
			Column('income_tax_expense', BIGINT(unsigned=False), nullable=False),
			Column('intangible_assets', BIGINT(unsigned=False), nullable=False),
			Column('interest_expense', BIGINT(unsigned=False), nullable=False),
			Column('interest_income', BIGINT(unsigned=False), nullable=False),
			Column('inventory', BIGINT(unsigned=False), nullable=False),
			Column('investments_in_property_plant_and_equipment', BIGINT(unsigned=False), nullable=False),
			Column('link', VARCHAR(255), nullable=True),
			Column('long_term_debt', BIGINT(unsigned=False), nullable=False),
			Column('long_term_investments', BIGINT(unsigned=False), nullable=False),
			Column('minority_interest', BIGINT(unsigned=False), nullable=False),
			Column('net_cash_provided_by_operating_activities', BIGINT(unsigned=False), nullable=False),
			Column('net_cash_used_for_investing_activites', BIGINT(unsigned=False), nullable=False),
			Column('net_cash_used_provided_by_financing_activities', BIGINT(unsigned=False), nullable=False),
			Column('net_change_in_cash', BIGINT(unsigned=False), nullable=False),
			Column('net_debt', BIGINT(unsigned=False), nullable=False),
			Column('net_income', BIGINT(unsigned=False), nullable=False),
			Column('net_income_ratio', FLOAT(32), nullable=True),
			Column('net_receivables', BIGINT(unsigned=False), nullable=False),
			Column('operating_cash_flow', BIGINT(unsigned=False), nullable=False),
			Column('operating_expenses', BIGINT(unsigned=False), nullable=False),
			Column('operating_income', BIGINT(unsigned=False), nullable=False),
			Column('operating_income_ratio', FLOAT(32), nullable=True),
			Column('other_assets', BIGINT(unsigned=False), nullable=False),
			Column('other_current_assets', BIGINT(unsigned=False), nullable=False),
			Column('other_current_liabilities', BIGINT(unsigned=False), nullable=False),
			Column('other_expenses', BIGINT(unsigned=False), nullable=False),
			Column('other_investing_activites', BIGINT(unsigned=False), nullable=False),
			Column('other_financing_activites', BIGINT(unsigned=False), nullable=False),
			Column('other_liabilities', BIGINT(unsigned=False), nullable=False),
			Column('other_non_cash_items', BIGINT(unsigned=False), nullable=False),
			Column('other_non_current_assets', BIGINT(unsigned=False), nullable=False),
			Column('other_non_current_liabilities', BIGINT(unsigned=False), nullable=False),
			Column('other_working_capital', BIGINT(unsigned=False), nullable=False),
			Column('othertotal_stockholders_equity', BIGINT(unsigned=False), nullable=False),
			Column('period', CHAR(2), nullable=False),
			Column('preferred_stock', BIGINT(unsigned=False), nullable=False),
			Column('property_plant_equipment_net', BIGINT(unsigned=False), nullable=False),
			Column('purchases_of_investments', BIGINT(unsigned=False), nullable=False),
			Column('reported_currency', CHAR(3), nullable=False),
			Column('research_and_development_expenses', BIGINT(unsigned=False), nullable=False),
			Column('retained_earnings', BIGINT(unsigned=False), nullable=False),
			Column('revenue', BIGINT(unsigned=False), nullable=False),
			Column('sales_maturities_of_investments', BIGINT(unsigned=False), nullable=False),
			Column('selling_and_marketing_expenses', BIGINT(unsigned=False), nullable=False),
			Column('selling_general_and_administrative_expenses', BIGINT(unsigned=False), nullable=False),
			Column('short_term_debt', BIGINT(unsigned=False), nullable=False),
			Column('short_term_investments', BIGINT(unsigned=False), nullable=False),
			Column('stock_based_compensation', BIGINT(unsigned=False), nullable=False),
			Column('symbol', VARCHAR(50), nullable=False),
			Column('tax_assets', BIGINT(unsigned=False), nullable=False),
			Column('tax_payables', BIGINT(unsigned=False), nullable=False),
			Column('total_assets', BIGINT(unsigned=False), nullable=False),
			Column('total_current_assets', BIGINT(unsigned=False), nullable=False),
			Column('total_current_liabilities', BIGINT(unsigned=False), nullable=False),
			Column('total_debt', BIGINT(unsigned=False), nullable=False),
			Column('total_equity', BIGINT(unsigned=False), nullable=False),
			Column('total_investments', BIGINT(unsigned=False), nullable=False),
			Column('total_liabilities', BIGINT(unsigned=False), nullable=False),
			Column('total_liabilities_and_stockholders_equity', BIGINT(unsigned=False), nullable=False),
			Column('total_liabilities_and_total_equity', BIGINT(unsigned=False), nullable=False),
			Column('total_non_current_assets', BIGINT(unsigned=False), nullable=False),
			Column('total_non_current_liabilities', BIGINT(unsigned=False), nullable=False),
			Column('total_other_income_expenses_net', BIGINT(unsigned=False), nullable=False),
			Column('total_stockholders_equity', BIGINT(unsigned=False), nullable=False),
			Column('weighted_average_shs_out', BIGINT(unsigned=False), nullable=False),
			Column('weighted_average_shs_out_dil', BIGINT(unsigned=False), nullable=False),
		)

	def fetch_sql_types(self, df: pandas.DataFrame) -> Tuple[Column]:
		"""Fetches SQL types for columns in passed DF

		Args:
			df (pandas.DataFrame): DF to fetch applicable sql types for.

		Returns:
			Tuple[Column]: Tuple containing Columns with SQL meta data.
		"""

		return tuple(_tuple for _tuple in self.sql_types if _tuple.key in df.columns.values)

	def fetch_pandas_types(self, df: pandas.DataFrame) -> Dict[str, DtypeArg]:
		"""Fetches pandas types for columns in passed DF

		Args:
			df (pandas.DataFrame): DF to fetch pandas applicable types for.

		Returns:
			Dict[str, DtypeArg]: Mapping column names to pandas dtypes
		"""

		return {k: v for k, v in self.pandas_types.items() if k in df.columns.values}

	@BaseTransformer.log_call
	def transform_financial_statement(
		self,
		df: pandas.DataFrame,
	) -> pandas.DataFrame:
		df = self.camel_case_to_snake_case_df_columns(df)

		df.dropna(
			subset=[
			'calendar_year',
			'date',
			'period',
			'reported_currency',
			'symbol',
			],
			inplace=True,
		)

		df['id'] = df['symbol'] + df['calendar_year'] + df['period']

		#Round all int type columns with "." in them to 0 decimal places and fillna = 0
		columns_to_round = [k for k, v in self.fetch_pandas_types(df=df).items() if v == numpy.int64]
		for col in columns_to_round:
			df[col].fillna(0, inplace=True)
			if any(df[col].astype(str).str.contains('\.')):
				df[col] = df[col].round(0)

		df.sort_values(by='accepted_date', ascending=True, inplace=True)
		df.drop_duplicates(subset=['id'], keep='last', inplace=True)

		return df
