from os import path, remove
from polars import DataFrame
from unittest import TestCase

from sample.export.user import \
  exporter as ex_can_user


class TestExporter(TestCase):
  def test_get_dfs(self) -> None:
    # Execute get_dfs() method
    dfs: dict[str, DataFrame] = ex_can_user.get_dfs()

    # Assert output is as expected
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == ['table']
    assert isinstance(dfs['table'], DataFrame)
  
  def test_transform(self) -> None:
    # Retrieve dfs as a dependent
    dfs: dict[str, DataFrame] = ex_can_user.get_dfs()

    # Execute transform() method
    df: DataFrame = ex_can_user.transform(dfs)

    # Assert output is as expected
    assert isinstance(df, DataFrame)
  
  def test_export(self) -> None:
    # Export and retrieve the resulting file name
    file_path: str = ex_can_user.export(ex_can_user.table.get())

    # Assert output is as expected
    assert file_path.endswith(ex_can_user.export_format.value)
    assert path.exists(file_path)

    # Run post-assertion cleanup
    remove(file_path)
