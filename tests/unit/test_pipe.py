from unittest import TestCase

from sample.standard.raw.activity import pipe as pip_raw_activity
from sample.standard.conformed.activity import pipe as pip_con_activity


class TestPipe(TestCase):
  def test_ingester_pipe(self) -> None:
    # Clean up dependent table first
    pip_raw_activity.table.truncate()

    # Execute raw pipe and ensure it loaded correctly
    row_count: int = pip_raw_activity.execute().shape[0]
    assert row_count == 2
  
  def test_transformer_pipe(self) -> None:
    # Clean up dependent table first
    pip_raw_activity.table.truncate()
    pip_raw_activity.execute()

    # Execute pipe and ensure it loaded correctly
    pip_con_activity.table.truncate()
    row_count: int = pip_con_activity.execute().shape[0]
    assert row_count == 3
  
  def test_exporter_pipe(self) -> None:
    ...
