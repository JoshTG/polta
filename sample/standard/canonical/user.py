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
    Field('active_ind', 'boolean'),
    Field('salary', 'long')
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
  from sample.standard.conformed.activity import \
    table as tab_con_activity
  from sample.standard.conformed.name import \
    table as tab_con_name
  from sample.standard.conformed.salary import \
    table as tab_con_salary

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
  salary_df: DataFrame = (tab_con_salary
    .get()
    .sort('_file_path', '_file_mod_ts', descending=True)
    .unique(subset='id', keep='first')
  )

  return {
    'activity': activity_df,
    'name': name_df,
    'salary': salary_df
  }

transform: str = '''
  SELECT
      n.*
    , a.active_ind
    , s.salary
  FROM
    name n
  INNER JOIN activity a
  ON n.id = a.id
  LEFT JOIN salary s
  ON n.id = s.id
'''

transformer: Transformer = Transformer(
  table=table,
  load_logic=load_dfs,
  transform_logic=transform,
  write_logic=WriteLogic.UPSERT
)

pipe: Pipe = Pipe(transformer)
