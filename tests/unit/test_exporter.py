from os import path, remove
from polars import DataFrame
from unittest import TestCase

from tests.unit.testing_data.exporter import TestingData


class TestExporter(TestCase):
  """Tests the Exporter class"""
  td: TestingData = TestingData()

  def test_get_dfs(self) -> None:
    # Execute get_dfs() method
    dfs: dict[str, DataFrame] = self.td.exporter.get_dfs()

    # Assert output is as expected
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == [self.td.exporter.table.id]
    assert isinstance(dfs[self.td.exporter.table.id], DataFrame)
  
  def test_transform(self) -> None:
    # Retrieve dfs as a dependent
    dfs: dict[str, DataFrame] = self.td.exporter.get_dfs()

    # Execute transform() method
    df: DataFrame = self.td.exporter.transform(dfs)

    # Assert output is as expected
    assert isinstance(df, DataFrame)
  
  def test_export(self) -> None:
    # Export and retrieve the resulting file name
    file_path: str = self.td.exporter \
      .export(self.td.exporter.table.get())

    # Assert output is as expected
    assert file_path.endswith(self.td.exporter.export_format.value)
    assert path.exists(file_path)

    # Run post-assertion cleanup
    remove(file_path)

    # Assert malformed export format fails as expected
    self.assertRaises(
      NotImplementedError,
      self.td.malformed_exporter.export,
      self.td.exporter.table.get()
    )
