from dataclasses import dataclass, field
from datetime import datetime
from json import dump
from os import makedirs, path
from polars import DataFrame
from typing import Optional

from polta.enums import ExportFormat, WriteLogic
from polta.table import PoltaTable


@dataclass
class PoltaExporter:
  """Executes an export of a PoltaTable
  
  Args:
    table (PoltaTable): the main table for the export
    export_format (ExportFormat): how to save the data
    export_directory (str): where to save the data (default export volume)
  """
  table: PoltaTable
  export_format: ExportFormat
  export_directory: str = field(default_factory=lambda: '')
  write_logic: Optional[WriteLogic] = field(init=False)
  exported_files: list[str] = field(init=False)

  def __post_init__(self) -> None:
    self.export_directory: str = self.export_directory or path.join(
      self.table.metastore.volumes_directory,
      'exports',
      self.table.domain,
      self.table.quality.value,
      self.table.name
    )
    makedirs(self.export_directory, exist_ok=True)
    self.write_logic = None
    self.exported_files: list[str] = []

  def get_dfs(self) -> dict[str, DataFrame]:
    """
    """
    return {'table': self.table.get()}

  def transform(self, dfs: dict[str, DataFrame]) -> DataFrame:
    """
    """
    return dfs['table']
  
  def export(self, df: DataFrame) -> Optional[str]:
    """Exports the DataFrame to file storage

    Args:
      df (DataFrame): the DataFrame to export
    
    Returns:
      file_path (Optional[str]): if applicable, the resulting file_path
    """
    ts: str = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name: str = f'{self.table.name}.{ts}.{self.export_format.value}'
    file_path: str = path.join(self.export_directory, file_name)
    if self.export_format.value == ExportFormat.CSV.value:
      self._to_csv(df, file_path)
    elif self.export_format.value == ExportFormat.JSON.value:
      self._to_json(df, file_path)
    else:
      raise NotImplementedError()
    self.exported_files.append(file_path)
    return file_path

  def _to_csv(self, df: DataFrame, file_path: str) -> None:
    """Exports the DataFrame to a CSV format
    """
    df.write_csv(file_path)
  
  def _to_json(self, df: DataFrame, file_path: str) -> None:
    """Exports the DataFrame to a JSON format
    """
    dump(df.to_dicts(), file_path)
