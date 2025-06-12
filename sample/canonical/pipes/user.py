from polars import DataFrame

from polta.enums import LoadLogic
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from sample.conformed.name import \
  table as pt_con_name
from sample.conformed.activity import \
  table as pt_con_activity


class UserPipe(PoltaPipe):
  """Pipe to load user data into a canonical model"""
  def __init__(self, table: PoltaTable) -> None:
    super().__init__(table, LoadLogic.UPSERT)
  
  def load_dfs(self) -> dict[str, DataFrame]:
    """Basic load logic:
      1. Get conformed name data as DataFrame
      2. Deduplicate name df based on most recent IDs
      3. Get conformed activity data as DataFrame
      4. Deduplicate activity df based on most recent IDs
    
    Returns:
      dfs (dict[str, DataFrame]): the resulting data as 'name' and 'activity'
    """
    name_df: DataFrame = (pt_con_name
      .get()
      .sort('_file_path', '_file_mod_ts', descending=True)
      .unique(subset='id', keep='first')
    )
    activity_df: DataFrame = (pt_con_activity
      .get()
      .sort('_file_path', '_file_mod_ts', descending=True)
      .unique(subset='id', keep='first')
    )

    return {
      'name': name_df,
      'activity': activity_df
    }

  def transform(self) -> DataFrame:
    """Basic transformation logic:
      1. Inner join name and activity on 'id'
    
    Returns:
      df (DataFrame): the joined DataFrame
    """
    name_df: DataFrame = self.dfs['name']
    activity_df: DataFrame = self.dfs['activity']
    return (name_df
      .join(activity_df, 'id', 'inner')
    )
