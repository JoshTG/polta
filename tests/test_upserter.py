from unittest import TestCase

from polta.exceptions import IncorrectQuality
from polta.upserter import Upserter
from sample.standard.conformed.activity import \
  table as tab_con_activity
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
    ...
  
  def test_transform(self) -> None:
    ...
  
  def test_export(self) -> None:
    ...
