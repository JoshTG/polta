from unittest import TestCase

from tests.testing_data.upserter import TestingData


class TestUpserter(TestCase):
  """Tests the Upserter class"""
  td: TestingData = TestingData()

  def test_get_dfs(self) -> None:
    ...
  
  def test_transform(self) -> None:
    ...
  
  def test_export(self) -> None:
    ...
