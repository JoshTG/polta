from datetime import datetime
from polars import DataFrame, lit
from typing import Any

from polta.enums import LoadLogic
from polta.pipe import PoltaPipe
from tests.testing_data.table import TestingData as ttd



class TestingData:
  df_1_rows: list[dict[str, Any]] = [
    {
      '_raw_id': 'abc',
      '_conformed_id': 'def',
      '_canonicalized_id': 'ghi',
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
      'id': 1,
      'name': 'Spongebob Squarepants'
    },
    {
      '_raw_id': 'jkl',
      '_conformed_id': 'mno',
      '_canonicalized_id': 'pqr',
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
      'id': 2, 'name': 'Gary the Snail'
    },
    {
      '_raw_id': 'stu',
      '_conformed_id': 'vwx',
      '_canonicalized_id': 'yza',
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
      'id': 3,
      'name': 'Plankton'
    }
  ]
  df_1_columns: list[str] = [
    '_canonicalized_id',
    '_conformed_id',
    '_created_ts',
    '_modified_ts',
    '_raw_id',
    'id',
    'name'
  ]

  df_2_rows: list[dict[str, Any]] = [
    {
      '_raw_id': 'bcd',
      '_conformed_id': 'efg',
      '_canonicalized_id': 'hij',
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
      'id': 1,
      'active_ind': True
    },
    {
      '_raw_id': 'klm',
      '_conformed_id': 'nop',
      '_canonicalized_id': 'qrs',
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
      'id': 2, 'active_ind': False
    },
    {
      '_raw_id': 'tuv',
      '_conformed_id': 'wxy',
      '_canonicalized_id': 'zab',
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
      'id': 3, 'active_ind': False
    }, 
  ]
  df_2_columns: list[str] = [
    '_canonicalized_id',
    '_conformed_id',
    '_created_ts',
    '_modified_ts',
    '_raw_id',
    'active_ind',
    'id'
  ]

  tdf_columns: list[str] = [
    '_canonicalized_id',
    '_conformed_id',
    '_created_ts',
    '_modified_ts',
    '_raw_id',
    'active_ind',
    'id',
    'name'
  ]

  sdf_ids: list[int] = [1, 2, 3]
  sdf_test_id: int = 1
  sdf_test_active_ind: bool = True

class PipeTest(PoltaPipe):
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
    df: DataFrame = df_name.join(df_activity, 'id', 'inner', suffix='_right')
    df: DataFrame = df.drop([c for c in df.columns if c.endswith('_right')])
    return df
