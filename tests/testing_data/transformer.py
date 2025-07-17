from polars import DataFrame
from typing import Any


class TestingData:
  """Contains test data for the transformer"""
  dfs: dict[str, DataFrame] = {
    'activity': DataFrame([
      {'payload': '[\n  {\n    "id": 1,\n    "active_ind": true\n  },\n  {\n    "id": 2,\n    "active_ind": false\n  }\n]'},
      {'payload': '[\n  {\n    "id": 3,\n    "active_ind": false\n  }\n]'}
    ])
  }
  output_rows: list[dict[str, Any]] = [
    {'id': '1', 'active_ind': True},
    {'id': '2', 'active_ind': False},
    {'id': '3', 'active_ind': False}
  ]
