from typing import Any


class DataTypeNotRecognized(Exception):
  def __init__(self, data_type: str) -> None:
    self.message: str = data_type
    super().__init__(self.message)

class DirectoryTypeNotRecognized(Exception):
  def __init__(self, directory_type: Any) -> None:
    self.message: str = directory_type
    super().__init__(self.message)

class EmptyPipeline(Exception):
  def __init__(self) -> None:
    self.message: str = f'Pipeline executed in strict mode but did not load data'
    super().__init__(self.message)

class LoadLogicNotRecognized(Exception):
  def __init__(self, load_logic: Any) -> None:
    self.message: str = f'Unrecognized load logic {str(load_logic)}'
    super().__init__(self.message)

class PoltaDataFormatNotRecognized(Exception):
  def __init__(self, format: type) -> None:
    self.message: str = f'Unrecognized instance type {format}'
    super().__init__(self.message)

class TableQualityNotRecognized(Exception):
  def __init__(self, quality: Any) -> None:
    self.message: str = f'Unrecognized table quality {quality}'
    super().__init__(self.message)
