from os import path, remove
from polars import DataFrame
from unittest import TestCase

from polta.enums import RawFileType, TableQuality
from polta.exceptions import (
  EmptyPipe, 
  TableQualityNotRecognized,
  WriteLogicNotRecognized
)
from sample.standard.raw.activity import \
  pipe as pip_raw_activity
from sample.standard.canonical.user import \
  table as tab_can_user
from sample.standard.conformed.activity import \
  pipe as pip_con_activity
from sample.standard.export.user import \
  pipe as pip_exp_user
from tests.testing_data.pipe import TestingData


class TestPipe(TestCase):
  """Tests the Pipe class via its three logic types"""
  td: TestingData = TestingData()

  def test_ingester_pipe(self) -> None:
    # Clean up dependent table first
    pip_raw_activity.table.truncate()

    # Execute raw pipe and assert it loaded correctly
    passed, failed, quarantined = pip_raw_activity.execute()
    assert passed.shape[0] == 2
    assert failed.shape[0] == 0
    assert quarantined.shape[0] == 0

    # Assert a malformed pipe fails to save
    self.assertRaises(WriteLogicNotRecognized, self.td.malformed_pipe.save, passed)

    # Assert a malformed table fails to be conformed to a layer
    self.td.malformed_pipe.table.quality = RawFileType.JSON
    self.assertRaises(TableQualityNotRecognized, self.td.malformed_pipe.add_metadata_columns, passed)
    self.td.malformed_pipe.table.quality = TableQuality.RAW

  def test_transformer_pipe(self) -> None:
    # Clean up dependent table first
    pip_raw_activity.table.truncate()
    pip_raw_activity.execute()

    # Execute pipe and assert it loaded correctly
    pip_con_activity.table.truncate()
    passed, failed, quarantined = pip_con_activity.execute()
    assert passed.shape[0] == 3
    assert failed.shape[0] == 0
    assert quarantined.shape[0] == 0
  
  def test_exporter_pipe(self) -> None:
    # Pre-assertion cleanup
    pip_exp_user.logic.exported_files.clear()

    # Execute export
    pip_exp_user.logic.export(tab_can_user.get())

    # Assert export worked as expected
    assert len(pip_exp_user.logic.exported_files) > 0
    for file_path in pip_exp_user.logic.exported_files:
      assert path.exists(file_path)
      remove(file_path)
    
    # Post-assertion cleanup
    pip_exp_user.logic.exported_files.clear()

  def test_strict_mode(self) -> None:
    # Pre-assertion cleanup
    pip_raw_activity.table.truncate()

    # Run in strict mode and assert it raises an empty pipe error
    self.assertRaises(EmptyPipe, pip_con_activity.execute, {}, False, True)

  def test_overwrite_save(self) -> None:
    # Pre-assertion cleanup
    self.td.overwrite_pipe.table.truncate()
    
    # Run with overwrite and ensure it runs correctly
    passed, failed, quarantined = self.td.overwrite_pipe.execute()
    assert isinstance(passed, DataFrame)
    assert passed.shape[0] == 2
    assert failed.shape[0] == 0
    assert quarantined.shape[0] == 0
  
  def test_execute_with_failures(self) -> None:
    # Execute pipe with passed, failed, and quarantined results
    passed, failed, quarantined = self.td.apply_test_pipe.execute(
      dfs={self.td.apply_test_table.id: self.td.apply_test_df},
      in_memory=True
    )

    # Assert passed DataFrame is as expected
    assert isinstance(passed, DataFrame)
    assert [r['id'] for r in passed.sort('id').to_dicts()] \
      == self.td.apply_test_passed_ids
    
    # Assert failed DataFrame is as expected
    assert isinstance(failed, DataFrame)
    assert failed.select('id', 'failed_test').sort('id').to_dicts() \
      == self.td.apply_test_failed_records
    
    # Assert quarantined DataFrame is as expected
    assert isinstance(quarantined, DataFrame)
    assert quarantined.select('id', 'failed_test').sort('id').to_dicts() \
      == self.td.apply_test_quarantined_records
