from deltalake import Field, Schema
from polars import DataFrame
from typing import Any

from polta.pipe import Pipe
from polta.table import Table, TableQuality
from polta.transformer import Transformer
from sample.metastore import metastore


table: Table = Table(
  domain='standard',
  quality=TableQuality.STANDARD,
  name='category',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string')
  ]),
  metastore=metastore,
  primary_keys=['id']
)

def load_dfs() -> dict[str, DataFrame]:
  """Generates a DataFrame from a hardcoded list of records

  Returns:
    dfs (dict[str, DataFrame]): the DataFrames to load
  """
  rows: list[dict[str, Any]] = [
    {'id': 'FT', 'name': 'Full Time'},
    {'id': 'PT', 'name': 'Part Time'},
    {'id': 'TP', 'name': 'Temporary'}
  ]
  return {table.id: DataFrame(rows, table.schema.raw_polars)}

def transform(dfs: dict[str, DataFrame]) -> DataFrame:
  """Converts the DataFrames to the singular crosswalk DataFrame

  Args:
    dfs (dict[str, DataFrame]): the loaded DataFrames
  
  Returns:
    df (DataFrame): the resulting DataFrame
  """
  return dfs[table.id]

transformer: Transformer = Transformer(
  table=table,
  load_logic=load_dfs,
  transform_logic=transform 
)

pipe: Pipe = Pipe(transformer)
