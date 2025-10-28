from polars import DataFrame
from unittest import TestCase

from polta.exceptions import IncorrectQuality
from polta.upserter import Upserter
from sample.standard.canonical.state import \
  pipe as pip_can_state
from sample.standard.conformed.activity import \
  table as tab_con_activity
from sample.standard.conformed.state import \
  pipe as pip_con_state
from sample.standard.raw.activity import \
  table as tab_raw_activity
from tests.testing_data.upserter import TestingData


class TestUpserter(TestCase):
  """Tests the Upserter class"""
  td: TestingData = TestingData()

  def test_init(self) -> None:
    # Assert a malformed upserter fails to save
    self.assertRaises(IncorrectQuality, Upserter, tab_con_activity, tab_raw_activity)

  def test_get_dfs(self) -> None:
    # Ensure source table is empty
    pip_can_state.table.truncate()

    # Retrieve the source DataFrame
    dfs: dict[str, DataFrame] = pip_can_state.logic.get_dfs()

    # Ensure dfs field is as expected
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == ['state']
    assert isinstance(dfs['state'], DataFrame)

  def test_transform(self) -> None:
    # Retrieve the source DataFrame
    assert pip_can_state.logic.transform(self.td.dfs).sort('id').to_dicts() == \
      self.td.output_rows

  def test_export(self) -> None:
    assert pip_can_state.logic.export(pip_can_state.table.get()) is None
  