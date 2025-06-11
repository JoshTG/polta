from deltalake import Field, Schema
from os import getcwd, path
from polars import col, DataFrame
from polars.datatypes import DataType, List, Struct

from polta.enums import LoadLogic, TableQuality
from polta.maps import PoltaMaps
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from polta.udfs import string_to_struct
from tests.testing_tables.raw.activity import activity_table as raw_table


activity_table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CONFORMED,
  name='activity',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('active_ind', 'boolean')
  ]),
  metastore_directory=path.join(getcwd(), 'tests', 'testing_tables', 'test_metastore')
)

class ActivityPipe(PoltaPipe):
  def __init__(self) -> None:
    super().__init__(activity_table, LoadLogic.APPEND)
    self.raw_polars_schema: dict[str, DataType] = PoltaMaps \
      .deltalake_schema_to_polars_schema(self.table.raw_schema)
  
  def load_dfs(self) -> dict[str, DataFrame]:
    conformed_ids: DataFrame = self.table.get(select=['_raw_id'], unique=True)
    df: DataFrame = (raw_table
      .get()
      .join(conformed_ids, '_raw_id', 'anti')
    )
    return {'activity': df}

  def transform(self) -> DataFrame:
    df: DataFrame = self.dfs['activity']

    return (df
      .with_columns([
        col('payload')
          .map_elements(string_to_struct, return_dtype=List(Struct(self.raw_polars_schema)))
      ])
      .explode('payload')
      .with_columns([
        col('payload').struct.field(f).alias(f)
        for f in [n.name for n in self.table.raw_schema.fields]
      ])
      .drop('payload')
    )

activity_pipe: ActivityPipe = ActivityPipe()
