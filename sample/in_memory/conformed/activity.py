from deltalake import Field, Schema
from polars import col, DataFrame
from polars.datatypes import DataType, List, Struct

from polta.enums import TableQuality, WriteLogic
from polta.maps import Maps
from polta.pipe import Pipe
from polta.table import Table
from polta.transformer import Transformer
from polta.udfs import string_to_struct
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
  """Basic transformation logic:
    1. Retrieve the raw activity DataFrame
    2. Convert 'payload' into a struct
    3. Explode the struct
    4. Convert the struct key-value pairs into column-cell values

  Returns:
    df (DataFrame): the resulting DataFrame
  """
  raw_polars_schema: dict[str, DataType] = Maps \
      .deltalake_schema_to_polars_schema(table.raw_schema)

  return (dfs[tab_raw_activity.id]
    .with_columns([
      col('payload')
        .map_elements(string_to_struct, return_dtype=List(Struct(raw_polars_schema)))
    ])
    .explode('payload')
    .with_columns([
      col('payload').struct.field(f).alias(f)
      for f in [n.name for n in table.raw_schema.fields]
    ])
    .drop('payload')
  )

transformer: Transformer = Transformer(
  table=table,
  load_logic=get_dfs,
  transform_logic=transform
)

pipe: Pipe = Pipe(transformer)
