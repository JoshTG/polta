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
  """Executes data transformation across two layers

  The main methods that should be overriden:
    1. load_dfs() -> populate self.dfs with dependent DataFrames
    2. transform() -> apply transformation logic against self.dfs
  
  The main method that should be executed:
    1. execute() -> executes the pipeline
  
  Args:
    table (PoltaTable): the destination Polta Table
    logic (Union[PoltaIngester, PoltaExporter, PoltaTransformer]): the pipe logic to handle data
    write_logic (WriteLogic): how the data should be placed in target table
    strict (optional) (bool): indicates whether to fail on empty target DataFrame
  
  Initialized fields:
    dfs (dict[str, DataFrame]): the dependent DataFrames for the pipeline
  """
  logic: Union[PoltaExporter, PoltaIngester, PoltaTransformer]
  strict: bool = field(default_factory=lambda: False)
  write_logic: Optional[WriteLogic] = field(init=False)
  table: PoltaTable = field(init=False)

  def __post_init__(self) -> None:
    self.table: PoltaTable = self.logic.table
    self.write_logic = self.logic.write_logic

  def execute(self) -> int:
    """Executes the pipe
    
    Returns:
      row_count (int): the number of records the pipeline wrote
    """
    dfs: dict[str, DataFrame] = self.logic.get_dfs()
    df: DataFrame = self.logic.transform(dfs)
    df: DataFrame = self.add_metadata_columns(df)
    df: DataFrame = self.conform_schema(df)

    if isinstance(self.logic, (PoltaIngester, PoltaTransformer)):
      self.save(df)
    elif isinstance(self.logic, PoltaExporter):
      self.logic.export(df)
  
    return df.shape[0]
  
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
    print(f'Loading {df.shape[0]} record(s) into {self.table.table_path}')

    if df.is_empty():
      if self.strict:
        raise EmptyPipe()
      return

    if self.write_logic.value == WriteLogic.APPEND.value:
      self.table.append(df)
    elif self.write_logic.value == WriteLogic.OVERWRITE.value:
      self.table.overwrite(df)
    elif self.write_logic.value == WriteLogic.UPSERT.value:
      self.table.upsert(df)
    else:
      raise WriteLogicNotRecognized(self.write_logic)
