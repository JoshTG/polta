from deltalake import Field, Schema
from polars import DataFrame

from polta.enums import TableQuality
from polta.pipe import Pipe
from polta.table import Table
from polta.transformer import Transformer
from sample.in_memory.raw.activity import \
  table as tab_raw_activity
from sample.metastore import metastore


table: Table = Table(
  domain='in_memory',
  quality=TableQuality.CONFORMED,
  name='activity',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('active_ind', 'boolean')
  ]),
  metastore=metastore
)

def get_dfs() -> dict[str, DataFrame]:
  return {}

def transform(dfs: dict[str, DataFrame]) -> DataFrame:
  """Returns the raw DataFrame

  Args:
    dfs (dict[str, DataFrame]): the input DataFrames

  Returns:
    df (DataFrame): the resulting DataFrame
  """
  return dfs[tab_raw_activity.id]

transformer: Transformer = Transformer(
  table=table,
  load_logic=get_dfs,
  transform_logic=transform
)

pipe: Pipe = Pipe(transformer)
