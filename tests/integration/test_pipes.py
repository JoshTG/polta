from os import path, remove
from unittest import TestCase

from polta.pipe import PoltaPipe

from sample.raw.activity import \
  pipe as pp_raw_activity
from sample.conformed.activity import \
  pipe as pp_con_activity
from sample.export.user import \
  pipe as pp_can_user


class TestPipes(TestCase):
  def test_ingester_pipe(self) -> None:
    # Pre-assertion cleanup
    pp_raw_activity.table.truncate()

    # Assert ingester pipe works as expected
    row_count: int = pp_raw_activity.execute()
    assert row_count == 2

    # Post-assertion cleanup
    pp_raw_activity.table.truncate()
  
  def test_transformer_pipe(self) -> None:
    # Pre-assertion dependency execution
    pp_raw_activity.table.truncate()
    pp_raw_activity.execute()

    # Assert transformer pipe works as expected
    row_count: int = pp_con_activity.execute()
    assert row_count == 3

    # Post-assertion cleanup
    pp_raw_activity.table.truncate()
    pp_con_activity.table.truncate()
  
  def test_exporter_pipe(self) -> None:
    # Assert exporter pipe works as expected
    pp_can_user.execute()
    assert len(pp_can_user.logic.exported_files) == 1
    assert path.exists(pp_can_user.logic.exported_files[0])

    # Post-assertion cleanup
    remove(pp_can_user.logic.exported_files[0])
    pp_can_user.logic.exported_files.clear()
