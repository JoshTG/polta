from dataclasses import dataclass, field
from polars import DataFrame
from typing import Optional

from polta.enums import TableQuality, PipeType
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
  """
  source_table: Table
  table: Table

  pipe_type: PipeType = field(init=False)

  def __post_init__(self) -> None:
    self.pipe_type: PipeType = PipeType.UPSERTER

    if self.source_table.quality.name not in [TableQuality.RAW.value, TableQuality.CONFORMED.value] \
     or self.table.quality.name == TableQuality.CANONICAL.value:
      raise IncorrectQuality()
    
  def get_dfs(self) -> dict[str, DataFrame]:
    """Executes the load_logic callable to return source DataFrames

    Returns:
      dfs (dict[str, DataFrame]): the source DataFrames
    """
    return {
      self.table.name: self.source_table.get()
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
