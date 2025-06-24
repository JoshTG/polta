from deltalake import Field, Schema
from polars import DataFrame

from polta.checks import *
from polta.test import Test
from polta.enums import CheckAction, TableQuality, WriteLogic
from polta.pipe import Pipe
from polta.table import Table
from polta.transformer import Transformer
from sample.metastore import metastore


table: Table = Table(
  domain='standard',
  quality=TableQuality.CANONICAL,
  name='user',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string'),
    Field('active_ind', 'boolean')
  ]),
  primary_keys=['id'],
  metastore=metastore,
  tests=[
    Test(check_not_null_or_empty, 'name', CheckAction.FAIL),
    Test(check_value_in, 'id', CheckAction.FAIL, {'values': ['2']})
  ]
)

def load_dfs() -> dict[str, DataFrame]:
  """Basic load logic:
    1. Get conformed name data as DataFrame
    2. Deduplicate name df based on most recent IDs
    3. Get conformed activity data as DataFrame
    4. Deduplicate activity df based on most recent IDs
  
  Returns:
    dfs (dict[str, DataFrame]): the resulting data as 'name' and 'activity'
  """
  from sample.standard.conformed.name import table as tab_con_name
  from sample.standard.conformed.activity import table as tab_con_activity

  name_df: DataFrame = (tab_con_name
    .get()
    .sort('_file_path', '_file_mod_ts', descending=True)
    .unique(subset='id', keep='first')
  )
  activity_df: DataFrame = (tab_con_activity
    .get()
    .sort('_file_path', '_file_mod_ts', descending=True)
    .unique(subset='id', keep='first')
  )

  return {
    'name': name_df,
    'activity': activity_df
  }

def transform(dfs: dict[str, DataFrame]) -> DataFrame:
  """Basic transformation logic:
    1. Inner join name and activity on 'id'
  
  Returns:
    df (DataFrame): the joined DataFrame
  """
  return dfs['name'].join(dfs['activity'], 'id', 'inner')

transformer: Transformer = Transformer(
  table=table,
  load_logic=load_dfs,
  transform_logic=transform,
  write_logic=WriteLogic.UPSERT
)

pipe: Pipe = Pipe(transformer)
