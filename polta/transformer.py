from dataclasses import dataclass
from polars import DataFrame
from typing import Optional

from polta.enums import WriteLogic
from polta.table import PoltaTable


@dataclass
class PoltaTransformer:
  table: PoltaTable
  load_logic: callable
  transform_logic: callable
  write_logic: WriteLogic

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
