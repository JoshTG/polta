import polars as pl

from dataclasses import dataclass, field
from polars import DataFrame
from typing import Optional

from polta.enums import TableQuality, PipeType, WriteLogic
from polta.exceptions import IncorrectQuality
from polta.table import Table


@dataclass
class Upserter:
  """Contains upsert logic to be used in a Pipe
  
  Positional Args:
    source_table (Table): the source table
    table (Table): the target Table
  
  Initialized fields:
    pipe_type (PipeType): the type of pipe this is (i.e., UPSERTER)
    write_logic (WriteLogic): the pipe type's write logic (i.e., UPSERT)
  """
  source_table: Table
  table: Table

  pipe_id: str = field(init=False)
  pipe_type: PipeType = field(init=False)
  write_logic: WriteLogic = field(init=False)

  def __post_init__(self) -> None:
    self.pipe_id: str = ''
    self.pipe_type: PipeType = PipeType.UPSERTER
    self.write_logic: WriteLogic = WriteLogic.UPSERT

    if self.source_table.quality.value not in [TableQuality.RAW.value, TableQuality.CONFORMED.value] \
     or self.table.quality.value != TableQuality.CANONICAL.value:
      raise IncorrectQuality()
        
  def get_dfs(self) -> dict[str, DataFrame]:
    """Executes the load_logic callable to return source DataFrames

    Returns:
      dfs (dict[str, DataFrame]): the source DataFrames
    """
    # Retrieve the source DataFrame
    df: DataFrame = self.source_table.get()
    id_col: str = '_conformed_id' if '_conformed_id' in df.columns else '_raw_id'
    upsert_df: DataFrame = self.table.metastore.get_upsert_history(self.pipe_id).rename({'_source_id': id_col})
    df: DataFrame = df.join(upsert_df, id_col, 'anti').select(*df.columns)

    # Return the resulting DataFrame
    return {
      self.table.name: df
    }

  def transform(self, dfs: dict[str, DataFrame]) -> DataFrame:
    """Applies the transform_logic callable to the DataFrames

    Args:
      dfs (dict[str, DataFrame]): the DataFrames to transform

    Returns:
      df (DataFrame): the transformed DataFrame
    """
    return (dfs[self.table.name]
      .sort('_file_path', '_file_mod_ts', descending=True)
      .unique(subset=self.table.primary_keys, keep='first')
    )
  
  def export(self, df: DataFrame) -> Optional[str]:
    """Exports the DataFrame in a desired format

    This method is unused for transformers

    Args:
      df (DataFrame): the DataFrame to export
    """
    return None
