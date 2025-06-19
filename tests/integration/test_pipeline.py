from os import path, remove
from polars import DataFrame
from unittest import TestCase

from sample.in_memory.pipelines.user import \
  pipeline as ppl_in_memory_user
from sample.standard.canonical.user import \
  table as tab_can_user
from sample.standard.conformed.activity import \
  table as tab_con_activity
from sample.standard.conformed.name import \
  table as tab_con_name
from sample.standard.pipelines.user import \
  pipeline as ppl_standard_user
from sample.standard.raw.activity import \
  table as tab_raw_activity


class TestPipeline(TestCase):
  """Tests the standard and in-memory Pipeline logic"""
  def test_standard_pipeline(self) -> None:
    # Truncate tables first
    tab_raw_activity.truncate()
    tab_con_activity.truncate()
    tab_con_name.truncate()
    tab_can_user.truncate()

    # Execute standard pipeline
    ppl_standard_user.execute()

    # Get resulting Delta Tables as DataFrames
    df_activity_raw: DataFrame = tab_raw_activity.get()
    df_activity_conformed: DataFrame = tab_con_activity.get()
    df_name: DataFrame = tab_con_name.get()
    df_user: DataFrame = tab_can_user.get()

    # Assert proper lengths
    assert df_activity_raw.shape[0] == 2
    assert df_activity_conformed.shape[0] == 3
    assert df_name.shape[0] == 3
    assert df_user.shape[0] == 3

    # Assert export worked as expected
    exported_files: list[str] = ppl_standard_user \
      .export_pipes[0].logic.exported_files
    assert len(exported_files) > 0
    for file_path in exported_files:
      assert path.exists(file_path)
      remove(file_path)
    
    # Post-assertion cleanup
    exported_files.clear()

  def test_in_memory_pipeline(self) -> None:
    # Execute in-memory pipeline
    ppl_in_memory_user.execute(in_memory=True)

    # Assert export worked as expected
    exported_files: list[str] = ppl_in_memory_user \
      .export_pipes[0].logic.exported_files
    assert len(exported_files) > 0
    for file_path in exported_files:
      assert path.exists(file_path)
      remove(file_path)
    
    # Post-assertion cleanup
    exported_files.clear()
