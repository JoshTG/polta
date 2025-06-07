from polars import col, DataFrame
from typing import Any
from unittest import TestCase

from polta.pipe import PoltaPipe
from tests.testing_data.pipe import TestingData, TestPipe


class TestPipe(TestCase):
  td: TestingData = TestingData()
  pipeline: PoltaPipe = TestPipe()
  df_1: DataFrame = DataFrame(td.df_1_rows)
  df_2: DataFrame = DataFrame(td.df_2_rows)

  def test_load_dfs(self) -> None:
    dfs: dict[str, DataFrame] = self.pipeline.load_dfs()
    # Assert dfs is the proper type and size
    assert isinstance(dfs, dict)
    assert len(dfs.keys()) == 2

    # Assert 'name' DataFrame is as expected
    df_name: DataFrame = dfs.get('name')
    assert isinstance(df_name, DataFrame)
    assert df_name.shape[0] == 3
    assert sorted(df_name.columns) == self.td.df_1_columns

    # Assert 'activity' DataFrame is as expected
    df_activity: DataFrame = dfs.get('activity')
    assert isinstance(df_activity, DataFrame)
    assert df_activity.shape[0] == 3
    assert sorted(df_activity.columns) == self.td.df_2_columns
  
  def test_transform(self) -> None:
    # Assert transformation happens as expected
    df: DataFrame = self.pipeline.transform()
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 3
    assert sorted(df.columns) == self.td.tdf_columns
  
  def test_save(self) -> None:
    # Ensure table is empty before testing
    self.pipeline.table.truncate()

    # Retrieve transformation result and saves result
    df: DataFrame = self.pipeline.transform()
    self.pipeline.save(df)

    # Retrieve resulting DataFrame and test type, size, and columns
    df_res: DataFrame = self.pipeline.table.get()
    assert isinstance(df_res, DataFrame)
    assert df_res.shape[0] == 3
    assert sorted(df_res.columns) == self.td.tdf_columns

    # Test IDs
    rows: list[dict[str, Any]] = df_res.to_dicts()
    ids: list[int] = [r['id'] for r in rows]
    assert sorted(ids) == self.td.sdf_ids

    # Test specific active_ind value
    test_active_ind: bool = (df_res
      .filter(col('id') == self.td.sdf_test_id)
      .select('active_ind')
      .to_dict(as_series=True)
      .get('active_ind', [])
      [-1]
    )
    assert test_active_ind == self.td.sdf_test_active_ind