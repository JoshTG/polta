from datetime import datetime, UTC
from polars import DataFrame


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
    'source': DataFrame([
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
