from polars import DataFrame
from unittest import TestCase

from sample.raw.activity import pipe as pp_raw_activity
from sample.conformed.activity import pipe as pp_con_activity

class TestPipe(TestCase):
  def test_ingester_pipe(self) -> None:
    # Clean up dependent table first
    pp_raw_activity.table.truncate()

    # Execute raw pipe and ensure it loaded correctly
    row_count: int = pp_raw_activity.execute()
    assert row_count == 2
  
  def test_transformer_pipe(self) -> None:
    # Clean up dependent table first
    pp_raw_activity.table.truncate()
    pp_raw_activity.execute()

    # Execute pipe and ensure it loaded correctly
    pp_con_activity.table.truncate()
    row_count: int = pp_con_activity.execute()
    assert row_count == 3
  
  def test_exporter_pipe(self) -> None:
    ...
