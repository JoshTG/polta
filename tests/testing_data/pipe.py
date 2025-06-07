from polars import DataFrame
from typing import Any

from polta.enums import LoadLogic
from polta.pipe import PoltaPipe
from tests.testing_data.table import TestingData as ttd



class TestingData:
  df_1_rows: list[dict[str, Any]] = [
    {'id': 1, 'name': 'Spongebob Squarepants'},
    {'id': 2, 'name': 'Gary the Snail'},
    {'id': 3, 'name': 'Plankton'}
  ]
  df_1_columns: list[str] = ['id', 'name']

  df_2_rows: list[dict[str, Any]] = [
    {'id': 1, 'active_ind': True},
    {'id': 2, 'active_ind': False},
    {'id': 3, 'active_ind': False}, 
  ]
  df_2_columns: list[str] = ['active_ind', 'id']

  tdf_columns: list[str] = ['active_ind', 'id', 'name']

  sdf_ids: list[int] = [1, 2, 3]
  sdf_test_id: int = 1
  sdf_test_active_ind: bool = True

class TestPipe(PoltaPipe):
  def __init__(self) -> None:
    super().__init__(
      table=ttd.table,
      load_logic=LoadLogic.OVERWRITE,
      strict=True
    )
  
  def load_dfs(self) -> dict[str, DataFrame]:
    td: TestingData = TestingData()
    dfs: dict[str, DataFrame] = {}
    dfs['name'] = DataFrame(td.df_1_rows)
    dfs['activity'] = DataFrame(td.df_2_rows)
    return dfs

  def transform(self) -> DataFrame:
    df_name: DataFrame = self.dfs['name']
    df_activity: DataFrame = self.dfs['activity']
    return df_name.join(df_activity, 'id', 'inner')
 