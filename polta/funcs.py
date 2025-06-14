import polars as pl
from typing import Union


def dedupe(df: pl.DataFrame, keys: Union[str, list[str]],
           order_by: Union[str, list[str]],
           descending: Union[bool, list[bool]] = False, 
           nulls_last: bool = False) -> pl.DataFrame:
  """Deduplicates a DataFrame by a list of keys and order sequence
  
  Args:
    df (DataFrame): the DataFrame to dedupe
    keys (Union[str, list[str]]): the keys to determine unique rows
    order_by (Union[str, list[str]]): how to determine which rows should stay
    descending (Union[bool, list[bool]]): whether to sort by descending (default False)
    nulls_last (bool): indicates whether to put nulls last in order (default False)

  Returns:
    df (DataFrame): the deduped DataFrame
  """
  # Enforce list type
  if isinstance(keys, str):
    keys: list[str] = [keys]
  if isinstance(order_by, str):
    order_by: list[str] = [order_by]
  if isinstance(descending, bool):
    descending: list[bool] = [descending]

  # ValueError validation checks
  if not isinstance(df, pl.DataFrame):
    raise TypeError('Error: df must be of type <DataFrame>')
  if not all(isinstance(k, str) for k in keys):
    raise TypeError('Error: keys must be of type <list[str]> or <str>')
  if not all(isinstance(c, str) for c in order_by):
    raise TypeError('Error: order_by must be of type <list[str]> or <str>')
  if not all(isinstance(d, bool) for d in descending):
    raise TypeError('Error: descending must be of type <list[bool]> or <bool>')
  
  return (df
    .sort(order_by, descending=descending, nulls_last=nulls_last)
    .unique(subset=keys, keep='first')
  )
