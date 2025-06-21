from dataclasses import dataclass, field
from inspect import signature


@dataclass
class Check:
  name: str
  description: str
  function: callable

  simple_function: bool = field(init=False)

  def __post_init__(self) -> None:
    self.simple_function: bool = len(signature(self.function).parameters) == 2

  def build_result_column(self, column: str) -> str:
    """Builds the result column name based on an input column
    
    Args:
      column (str): the column that was checked
    
    Returns:
      result_column (str): the result column name
    """
    return f'__{column}__{self.name}__'
