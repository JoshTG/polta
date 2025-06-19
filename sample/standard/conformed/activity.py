from deltalake import Field, Schema
from polars import col, DataFrame
from polars.datatypes import DataType, List, Struct

from polta.enums import TableQuality, WriteLogic
from polta.maps import PoltaMaps
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from polta.transformer import PoltaTransformer
from polta.udfs import string_to_struct
from sample.metastore import metastore


table: PoltaTable = PoltaTable(
  domain='standard',
  quality=TableQuality.CONFORMED,
  name='activity',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('active_ind', 'boolean')
  ]),
  metastore=metastore
)

def get_dfs() -> dict[str, DataFrame]:
  """Basic load logic:
    1. Get raw activity data as a DataFrame
    2. Anti join against conformed layer to get net-new records
  
  Returns:
    dfs (dict[str, DataFrame]): The resulting data as 'activity'
  """
  from sample.standard.raw.activity import table as tab_raw_activity

  conformed_ids: DataFrame = table.get(select=['_raw_id'], unique=True)
  df: DataFrame = (tab_raw_activity
    .get()
    .join(conformed_ids, '_raw_id', 'anti')
  )
  return {'activity': df}

def transform(dfs: dict[str, DataFrame]) -> DataFrame:
  """Basic transformation logic:
    1. Retrieve the raw activity DataFrame
    2. Convert 'payload' into a struct
    3. Explode the struct
    4. Convert the struct key-value pairs into column-cell values

  Returns:
    df (DataFrame): the resulting DataFrame
  """
  raw_polars_schema: dict[str, DataType] = PoltaMaps \
      .deltalake_schema_to_polars_schema(table.raw_schema)

  return (dfs['activity']
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

transformer: PoltaTransformer = PoltaTransformer(
  table=table,
  load_logic=get_dfs,
  transform_logic=transform,
  write_logic=WriteLogic.APPEND
)

pipe: PoltaPipe = PoltaPipe(transformer)
