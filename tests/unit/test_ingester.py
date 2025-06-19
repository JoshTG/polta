from os import getcwd, path
from polars import DataFrame
from unittest import TestCase

from polta.exceptions import DirectoryTypeNotRecognized
from polta.ingester import Ingester
from sample.standard.raw.activity import \
  ingester as ing_raw_activity
from tests.unit.testing_data.ingester import TestingData


class TestIngester(TestCase):
  """Tests the Ingester class"""
  td: TestingData = TestingData()
  ing: Ingester = ing_raw_activity

  def test_get_dfs(self) -> None:
    # Retrieve the dfs from the ingester
    dfs: dict[str, DataFrame] = self.ing.get_dfs()

    # Assert output is as expected
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == [self.ing.table.id]
    assert dfs[self.ing.table.id].shape[0] == self.td.expected_row_count
    assert dfs[self.ing.table.id].columns == self.td.expected_columns

    # Assert file metadata retrieval fails if a file does not exist
    bad_file_path: str = path.join(getcwd(), 'nonexistentdirectory', 'nonexistentfilename.txt')
    self.assertRaises(FileNotFoundError, self.ing._get_file_metadata, bad_file_path)

    # Assert malformed ingesters raise proper exceptions
    self.assertRaises(DirectoryTypeNotRecognized, self.td.malformed_dt_ingester.get_dfs)
    self.assertRaises(NotImplementedError, self.td.malformed_rft_ingester._ingest_files, dfs[self.ing.table.id])

  def test_transform(self) -> None:
    # Retrieve the transformed DataFrame
    df: DataFrame = self.ing.transform(self.td.dfs)

    # Assert output is as expected
    assert isinstance(df, DataFrame)
    assert df.shape[0] == self.td.expected_row_count

  def test_export(self) -> None:
    # Assert export method returns None
    assert self.ing.export(self.ing.table.get()) is None
