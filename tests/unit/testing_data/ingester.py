from datetime import datetime, UTC
from deltalake import Field, Schema
from polars import DataFrame

from polta.enums import DirectoryType, RawFileType, TableQuality
from polta.ingester import Ingester
from polta.table import Table
from sample.metastore import metastore
from sample.standard.raw.activity import \
  table as tab_raw_activity


class TestingData:
  expected_row_count: int = 2
  expected_columns: list[str] = [
    '_raw_id',
    '_ingested_ts',
    '_file_path',
    '_file_name',
    '_file_mod_ts',
    'payload'
  ]
  dfs: dict[str, DataFrame] = {
    'standard.raw.activity': DataFrame([
      {
        '_raw_id': '1',
        '_ingested_ts': datetime.now(UTC),
        '_file_path': 'path/to/file1.json',
        '_file_name': 'file1.json',
        '_file_mod_ts': datetime.now(UTC),
        'payload': '[]'
      },
      {
        '_raw_id': '2',
        '_ingested_ts': datetime.now(UTC),
        '_file_path': 'path/to/file2.json',
        '_file_name': 'file2.json',
        '_file_mod_ts': datetime.now(UTC),
        'payload': '[]'
      }
    ])
  }
  table: Table = Table(
    domain='standard',
    quality=TableQuality.RAW,
    name='activity',
    raw_schema=Schema([
      Field('payload', 'string'),
      Field('complex', 'string')
    ]),
    metastore=metastore
  )
  malformed_dt_ingester: Ingester = Ingester(
    table=tab_raw_activity,
    directory_type=RawFileType.JSON,
    raw_file_type=RawFileType.JSON
  )
  malformed_rft_ingester: Ingester = Ingester(
    table=table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=DirectoryType.SHALLOW
  )
