from deltalake import Field, Schema
from polars import DataFrame

from polta.enums import TableQuality
from polta.pipe import Pipe
from polta.table import Table
from polta.transformer import Transformer
from sample.in_memory.conformed.activity import \
  table as tab_con_activity
from sample.in_memory.conformed.name import \
  table as tab_con_name
from sample.metastore import metastore


table: Table = Table(
  domain='in_memory',
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
  return {}

def transform(dfs: dict[str, DataFrame]) -> DataFrame:
  """Basic transformation logic:
    1. Inner join name and activity on 'id'
  
  Returns:
    df (DataFrame): the joined DataFrame
  """
  return (dfs[tab_con_name.id]
    .join(dfs[tab_con_activity.id], 'id', 'inner')
  )

transformer: Transformer = Transformer(
  table=table,
  load_logic=load_dfs,
  transform_logic=transform
)

pipe: Pipe = Pipe(transformer)
