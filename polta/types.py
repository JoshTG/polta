from datetime import datetime
from polars import DataFrame, LazyFrame
from typing import Literal, TypeAlias, TypedDict, Union


ExcelSpreadsheetEngine: TypeAlias = Literal['calamine', 'openpyxl', 'xlsx2csv']
RawPoltaData: TypeAlias = Union[DataFrame, LazyFrame, dict, list[dict]]
Frame: TypeAlias = Union[DataFrame, LazyFrame]

class RawMetadata(TypedDict):
  """TypedDict to contain raw metadata for an ingest pipe"""
  _raw_id: str
  _ingested_ts: datetime
  _file_path: str
  _file_name: str
  _file_mod_ts: datetime
