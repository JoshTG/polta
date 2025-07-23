from os import getcwd, path
from polars import DataFrame, lit
from shutil import rmtree
from unittest import TestCase

from polta.exceptions import DirectoryTypeNotRecognized
from polta.ingester import Ingester
from polta.pipe import Pipe
from sample.standard.raw.activity import \
  ingester as ing_raw_activity
from tests.testing_data.ingester import TestingData


class TestIngester(TestCase):
  """Tests the Ingester class"""
  td: TestingData = TestingData()
  ing: Ingester = ing_raw_activity
  pip: Pipe = Pipe(ing)

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

  def test_get_file_paths(self) -> None:
    # Assert the test ingester finds two files to load
    assert len(self.ing._get_file_paths()) == 2

  def test_get_metadata(self) -> None:
    # Assert the test ingester retrieves two metadata files of proper format
    metadata: DataFrame = self.ing._get_metadata()
    assert isinstance(metadata, DataFrame)
    assert metadata.shape[0] == 2

  def test_filter_by_history(self) -> None:
    # Pre-assertion setup
    self.ing.table.truncate()
    if path.exists(self.ing.table.quarantine_path):
      rmtree(self.ing.table.quarantine_path)
    
    # Get source metadata and ensure neither record is filtered
    metadata: DataFrame = self.ing._get_metadata()
    res_1: DataFrame = self.ing._filter_by_history(metadata)
    assert res_1.shape[0] == 2

    # Write to the quarantine table and assert nothing gets filtered still
    quarantine_df: DataFrame = (metadata
      .limit(1)
      .with_columns(
        [
          lit('').alias('payload'),
          lit('__test__').alias('failed_test')
        ]
      )
    )
    self.pip.table.quarantine(quarantine_df)
    res_2: DataFrame = self.ing._filter_by_history(metadata)
    assert res_2.shape[0] == 2

    # Post-assertion cleanup
    self.ing.table.truncate()
    self.ing.table.clear_quarantine()