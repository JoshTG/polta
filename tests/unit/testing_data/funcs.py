from datetime import datetime
from polars import DataFrame


class TestingData:
  test_id: int = 1
  original_name: str = 'Spongebob'
  original_rowcount: int = 3
  updated_name: str = 'Spongebob Squarepants'
  updated_rowcount: int = 2
  df: DataFrame = DataFrame([
    {
      'id': 1,
      'name': updated_name,
      'updated_ts': datetime(2025, 1, 1)
    },
    {
      'id': 1,
      'name': original_name,
      'updated_ts': datetime(2024, 5, 7)
    },
    {
      'id': 2,
      'name': 'Plankton',
      'updated_ts': datetime(2025, 1, 7)
    }
  ])
