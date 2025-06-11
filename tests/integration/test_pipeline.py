from polars import DataFrame
from unittest import TestCase

from tests.testing_tables.canonical.user import user_table
from tests.testing_tables.conformed.activity import (
  activity_table as conformed_activity_table
)
from tests.testing_tables.conformed.name import name_table
from tests.testing_tables.pipeline import user_pipeline
from tests.testing_tables.raw.activity import (
  activity_table as raw_activity_table
)


class TestPipeline(TestCase):
  def test_pipeline(self) -> None:
    # Truncate tables first
    raw_activity_table.truncate()
    conformed_activity_table.truncate()
    name_table.truncate()
    user_table.truncate()

    # Execute pipeline
    user_pipeline.execute()

    # Get resulting Delta Tables as DataFrames
    df_activity_raw: DataFrame = raw_activity_table.get()
    df_activity_conformed: DataFrame = conformed_activity_table.get()
    df_name: DataFrame = name_table.get()
    df_user: DataFrame = user_table.get()

    # Assert proper lengths
    assert df_activity_raw.shape[0] == 2
    assert df_activity_conformed.shape[0] == 3
    assert df_name.shape[0] == 3
    assert df_user.shape[0] == 3
