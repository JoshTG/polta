import polars as pl

from dataclasses import dataclass, field
from datetime import datetime
from polars import DataFrame
from typing import Optional, Union
from uuid import uuid4

from polta.enums import WriteLogic, TableQuality
from polta.exceptions import (
  EmptyPipe,
  TableQualityNotRecognized,
  WriteLogicNotRecognized
)
from polta.exporter import PoltaExporter
from polta.ingester import PoltaIngester
from polta.table import PoltaTable
from polta.transformer import PoltaTransformer


@dataclass
class PoltaPipe:
  """Changes and moves data in the metastore
  
  Positional Args:
    logic (Union[PoltaIngester, PoltaExporter, PoltaTransformer]): the pipe logic to handle data
  
  Initialized fields:
    id (str): the unique ID of the pipe for the pipeline
    table (PoltaTable): the destination Polta Table
    write_logic (Optional[WriteLogic]): how the data should be placed in target table
  """
  logic: Union[PoltaExporter, PoltaIngester, PoltaTransformer]

  id: str = field(init=False)
  table: PoltaTable = field(init=False)
  write_logic: Optional[WriteLogic] = field(init=False)

  def __post_init__(self) -> None:
    self.id: str = '.'.join([
      'pp',
      self.logic.pipe_type.value,
      self.logic.table.id
    ])
    self.table: PoltaTable = self.logic.table
    self.write_logic = self.logic.write_logic

  def execute(self, dfs: dict[str, DataFrame] = {},
              in_memory: bool = False, strict: bool = False) -> DataFrame:
    """Executes the pipe

    Args:
      dfs (dict[str, DataFrame]): if applicable, source DataFrames (default {})
      in_memory (bool): indicates whether to run without saving (default False)
      strict (bool): indicates whether to fail on empty result (default False)

    Returns:
      df (DataFrame): the resulting DataFrame
    """
    dfs.update(self.logic.get_dfs())

    if isinstance(self.logic, PoltaExporter) and in_memory:
      df: DataFrame = dfs[self.table.id]
    else:
      df: DataFrame = self.logic.transform(dfs)
      df: DataFrame = self.add_metadata_columns(df)
      df: DataFrame = self.conform_schema(df)

    if strict and df.is_empty():
      raise EmptyPipe()

    if isinstance(self.logic, (PoltaIngester, PoltaTransformer)) and not in_memory:
      self.save(df)
    if isinstance(self.logic, PoltaExporter):
      self.logic.export(df)

    return df
  
  def add_metadata_columns(self, df: DataFrame) -> DataFrame:
    """Adds relevant metadata columns to the DataFrame before loading

    This method presumes the DataFrame carries its original metadata
    
    Args:
      df (DataFrame): the DataFrame before metadata columns
    
    Returns:
      df (DataFrame): the resulting DataFrame
    """
    id: str = str(uuid4())
    now: datetime = datetime.now()
    
    if self.table.quality.value == TableQuality.RAW.value:
      df: DataFrame = df.with_columns([
        pl.lit(id).alias('_raw_id'),
        pl.lit(now).alias('_ingested_ts')
      ])
    elif self.table.quality.value == TableQuality.CONFORMED.value:
      df: DataFrame = df.with_columns([
        pl.lit(id).alias('_conformed_id'),
        pl.lit(now).alias('_conformed_ts')
      ])
    elif self.table.quality.value == TableQuality.CANONICAL.value:
      df: DataFrame = df.with_columns([
        pl.lit(id).alias('_canonicalized_id'),
        pl.lit(now).alias('_created_ts'),
        pl.lit(now).alias('_modified_ts')
      ])
    else:
      raise TableQualityNotRecognized(self.table.quality.value)

    return df
  
  def conform_schema(self, df: DataFrame) -> DataFrame:
    """Conforms the DataFrame to the expected schema
    
    Args:
      df (DataFrame): the transformed, pre-conformed DataFrame
    
    Returns:
      df (DataFrame): the conformed DataFrame
    """
    df: DataFrame = self.add_metadata_columns(df)
    return df.select(*self.table.schema_polars.keys())

  def save(self, df: DataFrame) -> None:
    """Saves a DataFrame into the target Delta Table
    
    Args:
      df (DataFrame): the DataFrame to load
    """
    self.table.create_if_not_exists(
      table_path=self.table.table_path,
      schema=self.table.schema_deltalake
    )
    print(f'Loading {df.shape[0]} record(s) into {self.table.table_path}')

    if self.write_logic.value == WriteLogic.APPEND.value:
      self.table.append(df)
    elif self.write_logic.value == WriteLogic.OVERWRITE.value:
      self.table.overwrite(df)
    elif self.write_logic.value == WriteLogic.UPSERT.value:
      self.table.upsert(df)
    else:
      raise WriteLogicNotRecognized(self.write_logic)
