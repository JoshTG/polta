from dataclasses import dataclass, field
from polars import DataFrame

from polta.enums import LoadLogic
from polta.exceptions import EmptyPipeline, LoadLogicNotRecognized
from polta.table import PoltaTable


@dataclass
class PoltaPipeline:
  """Executes data transformation across layers

  The main methods that should be overriden:
    1. load_dfs() -> populate self.dfs with dependent DataFrames
    2. transform() -> apply transformation logic against self.dfs
  
  The main method that should be executed:
    1. execute() -> executes the pipeline
  
  Args:
    table (PoltaTable): the destination Polta Table
    load_logic (LoadLogic): how the data should be placed in target table
    strict (optional) (bool): indicates whether to fail on empty target DataFrame
  
  Initialized fields:
    dfs (dict[str, DataFrame]): the dependent DataFrames for the pipeline
  """
  table: PoltaTable
  load_logic: LoadLogic
  strict: bool = field(default_factory=lambda: False)
  dfs: dict[str, DataFrame] = field(init=False)

  def __post_init__(self) -> None:
    self.dfs: dict[str, DataFrame] = self.load_dfs()

  def execute(self) -> None:
    self.load_dfs()
    df: DataFrame = self.transform()
    df: DataFrame = self.conform_schema(df)
    self.save(df)
  
  def load_dfs(self) -> dict[str, DataFrame]:
    """Loads dependent DataFrames
    
    This should be overriden by a child class
    
    Returns:
      dfs (dict[str, DataFrame]): the dependent DataFrames
    """
    return {}

  def transform(self) -> DataFrame:
    """Transforms the dependent DataFrames into a pre-conformed DataFrame
    
    This should be overriden by a child class

    Returns:
      df (DataFrame): the transformed DataFrame
    """
    return DataFrame([], self.table.schema_polars)
  
  def conform_schema(self, df: DataFrame) -> DataFrame:
    """Conforms the DataFrame to the expected schema
    
    Args:
      df (DataFrame): the transformed, pre-conformed DataFrame
    
    Returns:
      df (DataFrame): the conformed DataFrame
    """
    return df.select(*self.table.schema_polars.keys())

  def save(self, df: DataFrame) -> None:
    """Saves a DataFrame into the target Delta Table
    
    Args:
      df (DataFrame): the DataFrame to load
    """
    print(f'Loading {df.shape[0]} record(s) into {self.table.table_path}')

    if df.is_empty():
      if self.strict:
        raise EmptyPipeline()
      return

    if self.load_logic.value == LoadLogic.APPEND.value:
      self.table.append(df)
    elif self.load_logic.value == LoadLogic.OVERWRITE.value:
      self.table.overwrite(df)
    elif self.load_logic.value == LoadLogic.UPSERT.value:
      self.table.upsert(df)
    else:
      raise LoadLogicNotRecognized(self.load_logic)
