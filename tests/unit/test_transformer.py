from polars import DataFrame
from unittest import TestCase

from polta.transformer import PoltaTransformer
from sample.conformed.activity import transformer
from sample.raw.activity import table as pt_raw_activity
from tests.unit.testing_data.transformer import TestingData


class TestTransformer(TestCase):
  td: TestingData = TestingData()
  ptr: PoltaTransformer = transformer

  def test_load_dfs(self) -> None:
    # Ensure source table is empty
    pt_raw_activity.truncate()

    dfs: dict[str, DataFrame] = self.ptr.get_dfs()
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == ['activity']
    assert isinstance(dfs['activity'], DataFrame)

  def test_transform(self) -> None:
    # Assert transform works as expected
    assert self.ptr.transform(self.td.dfs).to_dicts() == \
      self.td.output_rows

  def test_export(self) -> None:
    assert self.ptr.export(self.ptr.table.get()) is None
