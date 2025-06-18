from dataclasses import dataclass, field
from polars import DataFrame
from typing import Optional

from polta.enums import PipeType, WriteLogic
from polta.table import PoltaTable


@dataclass
class PoltaTransformer:
  """Contains transformation logic to be used in a PoltaPipe
  
  Positional Args:
    table (PoltaTable): the target PoltaTable
    load_logic (callable): a method to load the source DataFrames
    transform_logic (callable): a method to transform the DataFrames
    write_logic (WriteLogic): how to write to a PoltaTable (default APPEND)
  
  Initialized fields:
    pipe_type (PipeType): the type of pipe this is (i.e., TRANSFORMER)
  """
  table: PoltaTable
  load_logic: callable
  transform_logic: callable
  write_logic: WriteLogic = field(default_factory=lambda: WriteLogic.APPEND)

  pipe_type: PipeType = field(init=False)

  def __post_init__(self) -> None:
    self.pipe_type: PipeType = PipeType.TRANSFORMER

  def get_dfs(self) -> dict[str, DataFrame]:
    """Executes the load_logic callable to return source DataFrames

    Returns:
      dfs (dict[str, DataFrame]): the source DataFrames
    """
    return self.load_logic()

  def transform(self, dfs: dict[str, DataFrame]) -> DataFrame:
    """Applies the transform_logic callable to the DataFrames

    Args:
      dfs (dict[str, DataFrame]): the DataFrames to transform

    Returns:
      df (DataFrame): the transformed DataFrame
    """
    return self.transform_logic(dfs)

  def export(self, df: DataFrame) -> Optional[str]:
    """Exports the DataFrame in a desired format

    This method is unused for transformers

    Args:
      df (DataFrame): the DataFrame to export
    """
    return None
