from polars import DataFrame
from unittest import TestCase

from polta.transformer import Transformer
from sample.standard.conformed.activity import \
  transformer as tra_con_activity
from sample.standard.raw.activity import table as tab_raw_activity
from tests.unit.testing_data.transformer import TestingData


class TestTransformer(TestCase):
  """Tests Transformer class"""
  td: TestingData = TestingData()
  tr: Transformer = tra_con_activity

  def test_load_dfs(self) -> None:
    # Ensure source table is empty
    tab_raw_activity.truncate()

    dfs: dict[str, DataFrame] = self.tr.get_dfs()
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == ['activity']
    assert isinstance(dfs['activity'], DataFrame)

  def test_transform(self) -> None:
    # Assert transform works as expected
    assert self.tr.transform(self.td.dfs).to_dicts() == \
      self.td.output_rows

  def test_export(self) -> None:
    # Assert export returns None
    assert self.tr.export(self.tr.table.get()) is None
