from deltalake import Field, Schema
from polars import DataFrame

from polta.enums import TableQuality, WriteLogic
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from polta.transformer import PoltaTransformer
from sample.metastore import metastore


table: PoltaTable = PoltaTable(
  domain='standard',
  quality=TableQuality.CANONICAL,
  name='user',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string'),
    Field('active_ind', 'boolean')
  ]),
  primary_keys=['id'],
  metastore=metastore
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
  from sample.standard.conformed.name import table as pt_con_name
  from sample.standard.conformed.activity import table as pt_con_activity

  name_df: DataFrame = (pt_con_name
    .get()
    .sort('_file_path', '_file_mod_ts', descending=True)
    .unique(subset='id', keep='first')
  )
  activity_df: DataFrame = (pt_con_activity
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

transformer: PoltaTransformer = PoltaTransformer(
  table=table,
  load_logic=load_dfs,
  transform_logic=transform,
  write_logic=WriteLogic.UPSERT
)

pipe: PoltaPipe = PoltaPipe(transformer)
