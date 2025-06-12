from polars import col, DataFrame
from polars.datatypes import DataType, List, Struct

from polta.enums import LoadLogic
from polta.maps import PoltaMaps
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from polta.udfs import string_to_struct
from sample.raw.activity import \
  table as pt_raw_activity


class ActivityPipe(PoltaPipe):
  """Pipe to load activity data into a conformed model"""
  def __init__(self, table: PoltaTable) -> None:
    super().__init__(table, LoadLogic.APPEND)
    self.raw_polars_schema: dict[str, DataType] = PoltaMaps \
      .deltalake_schema_to_polars_schema(self.table.raw_schema)
  
  def load_dfs(self) -> dict[str, DataFrame]:
    """Basic load logic:
      1. Get raw activity data as a DataFrame
      2. Anti join against conformed layer to get net-new records
    
    Returns:
      dfs (dict[str, DataFrame]): The resulting data as 'activity'
    """
    conformed_ids: DataFrame = self.table.get(select=['_raw_id'], unique=True)
    df: DataFrame = (pt_raw_activity
      .get()
      .join(conformed_ids, '_raw_id', 'anti')
    )
    return {'activity': df}

  def transform(self) -> DataFrame:
    """Basic transformation logic:
      1. Retrieve the raw activity DataFrame
      2. Convert 'payload' into a struct
      3. Explode the struct
      4. Convert the struct key-value pairs into column-cell values

    Returns:
      df (DataFrame): the resulting DataFrame
    """
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
