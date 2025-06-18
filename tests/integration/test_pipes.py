from os import path, remove
from unittest import TestCase

from polta.pipe import PoltaPipe

from sample.standard.raw.activity import \
  pipe as pip_raw_activity
from sample.standard.conformed.activity import \
  pipe as pip_con_activity
from sample.standard.export.user import \
  pipe as pip_can_user


class TestPipes(TestCase):
  def test_ingester_pipe(self) -> None:
    # Pre-assertion cleanup
    pip_raw_activity.table.truncate()

    # Assert ingester pipe works as expected
    row_count: int = pip_raw_activity.execute().shape[0]
    assert row_count == 2

    # Post-assertion cleanup
    pip_raw_activity.table.truncate()
  
  def test_transformer_pipe(self) -> None:
    # Pre-assertion dependency execution
    pip_raw_activity.table.truncate()
    pip_raw_activity.execute()

    # Assert transformer pipe works as expected
    row_count: int = pip_con_activity.execute().shape[0]
    assert row_count == 3

    # Post-assertion cleanup
    pip_raw_activity.table.truncate()
    pip_con_activity.table.truncate()
  
  def test_exporter_pipe(self) -> None:
    # Assert exporter pipe works as expected
    pip_can_user.execute()
    assert len(pip_can_user.logic.exported_files) == 1
    assert path.exists(pip_can_user.logic.exported_files[0])

    # Post-assertion cleanup
    remove(pip_can_user.logic.exported_files[0])
    pip_can_user.logic.exported_files.clear()
