import polars as pl

from deltalake import Field, Schema
from polars import DataFrame
from os import getcwd, path

from polta.enums import LoadLogic, TableQuality
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from tests.testing_tables.conformed.name import name_table
from tests.testing_tables.conformed.activity import activity_table


user_table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CANONICAL,
  name='user',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string'),
    Field('active_ind', 'boolean')
  ]),
  primary_keys=['id'],
  metastore_directory=path.join(getcwd(), 'tests', 'testing_tables', 'test_metastore')
)

class UserPipe(PoltaPipe):
  def __init__(self) -> None:
    super().__init__(user_table, LoadLogic.UPSERT)
  
  def load_dfs(self) -> dict[str, DataFrame]:
    name_df: DataFrame = (name_table
      .get()
      .sort('_file_path', '_file_mod_ts', descending=True)
      .unique(subset='id', keep='first')
    )
    activity_df: DataFrame = (activity_table
      .get()
      .sort('_file_path', '_file_mod_ts', descending=True)
      .unique(subset='id', keep='first')
    )

    return {
      'name': name_df,
      'activity': activity_df
    }

  def transform(self) -> DataFrame:
    name_df: DataFrame = self.dfs['name']
    activity_df: DataFrame = self.dfs['activity']
    return (name_df
      .join(activity_df, 'id', 'inner')
    )

user_pipe: UserPipe = UserPipe()
