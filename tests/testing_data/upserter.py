from datetime import datetime, UTC
from polars import DataFrame
from typing import Any


class TestingData:
  dfs: dict[str, DataFrame] = {
    'state': DataFrame([
      {
        '_file_path': 'path/to/file1.json',
        '_file_mod_ts': datetime(2025, 1, 1, tzinfo=UTC),
        'id': 1,
        'state': 'PA'
      },
      {
        '_file_path': 'path/to/file2.json',
        '_file_mod_ts': datetime(2025, 1, 2, tzinfo=UTC),
        'id': 2,
        'state': 'CT'
      }
    ])
  }
  output_rows: list[dict[str, Any]] = [
    {
      '_file_path': 'path/to/file1.json',
      '_file_mod_ts': datetime(2025, 1, 1, tzinfo=UTC), 
      'id': 1,
      'state': 'PA'
    },
    {
      '_file_path': 'path/to/file2.json',
      '_file_mod_ts': datetime(2025, 1, 2, tzinfo=UTC),
      'id': 2,
      'state': 'CT'
    }
  ]