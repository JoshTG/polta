from dataclasses import dataclass, field
from polars import DataFrame
from typing import Optional

from polta.table import PoltaTable


@dataclass
class PoltaTransformer:
  table: PoltaTable
  load_logic: callable
  transform_logic: callable

  def get_dfs(self) -> dict[str, DataFrame]:
    """
    """
    return self.load_logic()

  def transform(self, df: DataFrame) -> DataFrame:
    return self.transform_logic(df)

  def export(self, df: DataFrame) -> Optional[str]:
    """
    """
    return None
