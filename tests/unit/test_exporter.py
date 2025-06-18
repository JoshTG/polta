from os import path, remove
from polars import DataFrame
from unittest import TestCase

from sample.standard.export.user import \
  exporter as exp_can_user


class TestExporter(TestCase):
  def test_get_dfs(self) -> None:
    # Execute get_dfs() method
    dfs: dict[str, DataFrame] = exp_can_user.get_dfs()

    # Assert output is as expected
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == [exp_can_user.table.id]
    assert isinstance(dfs[exp_can_user.table.id], DataFrame)
  
  def test_transform(self) -> None:
    # Retrieve dfs as a dependent
    dfs: dict[str, DataFrame] = exp_can_user.get_dfs()

    # Execute transform() method
    df: DataFrame = exp_can_user.transform(dfs)

    # Assert output is as expected
    assert isinstance(df, DataFrame)
  
  def test_export(self) -> None:
    # Export and retrieve the resulting file name
    file_path: str = exp_can_user.export(exp_can_user.table.get())

    # Assert output is as expected
    assert file_path.endswith(exp_can_user.export_format.value)
    assert path.exists(file_path)

    # Run post-assertion cleanup
    remove(file_path)
