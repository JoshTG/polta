from os import path, remove
from unittest import TestCase

from sample.standard.raw.activity import \
  pipe as pip_raw_activity
from sample.standard.conformed.activity import \
  pipe as pip_con_activity
from sample.standard.export.user import \
  pipe as pip_exp_user


class TestPipes(TestCase):
  """Tests the Pipe logic"""
  def test_ingester_pipe(self) -> None:
    # Pre-assertion cleanup
    pip_raw_activity.table.truncate()

    # Assert ingester pipe works as expected
    passed, _, _ = pip_raw_activity.execute()
    assert passed.shape[0] == 2

    # Post-assertion cleanup
    pip_raw_activity.table.truncate()
  
  def test_transformer_pipe(self) -> None:
    # Pre-assertion dependency execution
    pip_raw_activity.table.truncate()
    pip_raw_activity.execute()

    # Assert transformer pipe works as expected
    passed, _, _ = pip_con_activity.execute()
    assert passed.shape[0] == 3

    # Post-assertion cleanup
    pip_raw_activity.table.truncate()
    pip_con_activity.table.truncate()
  
  def test_exporter_pipe(self) -> None:
    # Execute export pipe
    pip_exp_user.execute()

    # Assert exporter pipe works as expected
    exported_files: list[str] = pip_exp_user.logic.exported_files
    assert len(pip_exp_user.logic.exported_files) > 0
    for file_path in exported_files:
      assert path.exists(file_path)
      remove(file_path)

    # Post-assertion cleanup
    pip_exp_user.logic.exported_files.clear()
