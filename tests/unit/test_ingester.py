from polars import DataFrame
from unittest import TestCase

from polta.ingester import PoltaIngester
from sample.standard.raw.activity import \
  ingester as ing_raw_activity
from tests.unit.testing_data.ingester import TestingData


class TestIngester(TestCase):
  td: TestingData = TestingData()
  ing: PoltaIngester = ing_raw_activity

  def test_get_dfs(self) -> None:
    dfs: dict[str, DataFrame] = self.ing.get_dfs()
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == [self.ing.table.id]
    assert dfs[self.ing.table.id].shape[0] == self.td.expected_row_count
    assert dfs[self.ing.table.id].columns == self.td.expected_columns

  def test_transform(self) -> None:
    df: DataFrame = self.ing.transform(self.td.dfs)
    assert isinstance(df, DataFrame)
    assert df.shape[0] == self.td.expected_row_count

  def test_export(self) -> None:
    assert self.ing.export(self.ing.table.get()) is None
