from .base_extractor import BaseExtractor
import pandas
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSheetsExtractor(BaseExtractor):

	def __init__(
		self,
		client_secret: dict,
	) -> None:
		"""Connector class to read/write off google sheets for the passed client secret

		Args:
			client_secret (dict): Client secret to use for reading/writing off google sheets.
		"""

		super().__init__(__name__)

		creds = ServiceAccountCredentials.from_json_keyfile_dict(
			json.loads(client_secret),
			scopes=['https://www.googleapis.com/auth/drive'],
		)

		self.client = gspread.authorize(creds)

	@BaseExtractor.log_call
	def list_sheets_in_spreadsheet(
		self,
		spread_sheet_name: str,
	) -> list[str]:
		"""Fetches list of sheet names in passed spreadsheet.

		Args:
			spread_sheet_name (str): Spreadsheet to retrieve list of sheet names for

		Returns:
			list[str]: List of sheet names in passed spreadsheet
		"""

		spread_sheet = self.client.open(spread_sheet_name)
		return [sheet.title for sheet in spread_sheet.worksheets()]

	@BaseExtractor.log_call
	def get_records_from_sheet(
		self,
		sheet_name: str,
		spread_sheet_name: str,
	) -> pandas.DataFrame:
		"""Fetches records from passed spreadsheet.sheet

		Args:
			sheet_name (str): Sheet to retrieve records from
			spread_sheet_name (str): Name of spreadsheet the sheet belongs to

		Returns:
			pandas.DataFrame: Return spreadsheet.sheet records as pandas dataframe.
		"""
		spread_sheet = self.client.open(spread_sheet_name)
		sheet = spread_sheet.worksheet(sheet_name)
		records = sheet.get_all_records()
		return pandas.DataFrame.from_records(records)
