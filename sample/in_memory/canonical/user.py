from deltalake import Field, Schema
from polars import DataFrame

from polta.enums import TableQuality, WriteLogic
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from polta.transformer import PoltaTransformer
from sample.in_memory.conformed.activity import \
  table as pt_con_activity
from sample.in_memory.conformed.name import \
  table as pt_con_name
from sample.metastore import metastore


table: PoltaTable = PoltaTable(
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
  return (dfs[pt_con_name.id]
    .join(dfs[pt_con_activity.id], 'id', 'inner')
  )

transformer: PoltaTransformer = PoltaTransformer(
  table=table,
  load_logic=load_dfs,
  transform_logic=transform
)

pipe: PoltaPipe = PoltaPipe(transformer)
