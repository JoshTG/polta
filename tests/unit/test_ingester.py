from polars import DataFrame
from unittest import TestCase

from polta.ingester import PoltaIngester
from sample.standard.raw.activity import ingester
from tests.unit.testing_data.ingester import TestingData


class TestIngester(TestCase):
  td: TestingData = TestingData()
  pin: PoltaIngester = ingester

  def test_get_dfs(self) -> None:
    dfs: dict[str, DataFrame] = self.pin.get_dfs()
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == [self.pin.table.id]
    assert dfs[self.pin.table.id].shape[0] == self.td.expected_row_count
    assert dfs[self.pin.table.id].columns == self.td.expected_columns

  def test_transform(self) -> None:
    df: DataFrame = self.pin.transform(self.td.dfs)
    assert isinstance(df, DataFrame)
    assert df.shape[0] == self.td.expected_row_count

  def test_export(self) -> None:
    assert self.pin.export(self.pin.table.get()) is None
