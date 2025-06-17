from os import path, remove
from polars import DataFrame
from unittest import TestCase

from sample.canonical.user import \
  table as pt_can_user
from sample.conformed.activity import \
  table as pt_con_activity
from sample.conformed.name import \
  table as pt_con_name
from sample.pipelines.user import \
  pipeline as ppl_user
from sample.raw.activity import \
  table as pt_raw_activity


class TestPipeline(TestCase):
  def test_pipeline(self) -> None:
    # Truncate tables first
    pt_raw_activity.truncate()
    pt_con_activity.truncate()
    pt_con_name.truncate()
    pt_can_user.truncate()

    # Execute pipeline
    ppl_user.execute()

    # Get resulting Delta Tables as DataFrames
    df_activity_raw: DataFrame = pt_raw_activity.get()
    df_activity_conformed: DataFrame = pt_con_activity.get()
    df_name: DataFrame = pt_con_name.get()
    df_user: DataFrame = pt_can_user.get()

    # Assert proper lengths
    assert df_activity_raw.shape[0] == 2
    assert df_activity_conformed.shape[0] == 3
    assert df_name.shape[0] == 3
    assert df_user.shape[0] == 3

    # Assert export worked as expected
    exported_files: list[str] = ppl_user.export_pipes[0].logic.exported_files
    assert len(exported_files) == 1
    assert path.exists(exported_files[0])
    remove(exported_files[0])
    exported_files.clear()
