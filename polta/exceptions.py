from typing import Any


class DataTypeNotRecognized(Exception):
  def __init__(self, data_type: str) -> None:
    self.message: str = data_type
    super().__init__(self.message)

class RawSchemaNotRecognized(Exception):
  def __init__(self, schema: Any) -> None:
    self.message: str = f'Unrecognized type {type(schema)} for {schema}'
    super().__init__(self.message)

class PoltaDataFormatNotRecognized(Exception):
  def __init__(self, format: type) -> None:
    self.message: str = f'Unrecognized instance type {format}'
    super().__init__(self.message)
