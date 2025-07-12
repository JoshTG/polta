from polars import DataFrame
from unittest import TestCase

from polta.enums import DirectoryType, RawFileType
from polta.ingester import Ingester
from sample.in_memory.conformed.name import \
  pipe as pip_con_name
from sample.standard.conformed.height import \
  ingester as ing_con_height
from sample.standard.raw.activity import \
  ingester as ing_raw_activity
from sample.standard.conformed.name import \
  ingester as ing_con_name

class TestIngest(TestCase):
  """Tests the Ingester class"""
  def test_simple_ingest(self) -> None:
    # Pre-assertion cleanup
    ing_raw_activity.table.truncate()

    # Assert simple ingest
    assert ing_raw_activity.simple_payload

    # Retrieve the DataFrames and ensure type
    dfs: dict[str, DataFrame] = ing_raw_activity.get_dfs()
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == [ing_raw_activity.table.id]

    # Retrieve the DataFrame and ensure it is as expected
    df: DataFrame = dfs[ing_raw_activity.table.id]
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 2
    assert '_raw_id' in df.columns
    assert '_ingested_ts' in df.columns
    assert '_file_path' in df.columns
    assert '_file_name' in df.columns
    assert '_file_mod_ts' in df.columns
    assert 'payload' in df.columns
    assert len(df.columns) == 6

  def test_json_ingest(self) -> None:
    # Pre-assertion cleanup
    ing_con_name.table.truncate()

    # Assert dated JSON ingest
    assert ing_con_name.directory_type.value \
      == DirectoryType.DATED.value
    assert not ing_con_name.simple_payload

    # Retrieve the DataFrames and ensure type
    dfs: dict[str, DataFrame] = ing_con_name.get_dfs()
    assert isinstance(dfs, dict)

    # Retrieve the DataFrame and ensure it is as expected
    df: DataFrame = dfs[ing_con_name.table.id]
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 3
    assert '_raw_id' in df.columns
    assert '_conformed_id' in df.columns
    assert '_conformed_ts' in df.columns
    assert '_ingested_ts' in df.columns
    assert '_file_path' in df.columns
    assert '_file_name' in df.columns
    assert '_file_mod_ts' in df.columns
    assert 'id' in df.columns
    assert 'name' in df.columns
    assert len(df.columns) == 9

  def test_csv_ingest(self) -> None:
    # Pre-assertion cleanup
    ing_con_height.table.truncate()

    # Assert complex shallow Excel ingestion
    assert ing_con_height.raw_file_type.value \
      == RawFileType.CSV.value
    assert ing_con_height.directory_type.value \
      == DirectoryType.SHALLOW.value
    assert not ing_con_height.simple_payload

    # Retrieve the DataFrames and ensure type
    dfs: dict[str, DataFrame] = ing_con_height.get_dfs()
    assert isinstance(dfs, dict)

    # Retrieve the DataFrame and ensure it is as expected
    df: DataFrame = dfs[ing_con_height.table.id]
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 3
    assert 'id' in df.columns
    assert 'height' in df.columns

  def test_excel_ingest(self) -> None:
    # Pre-assertion cleanup
    pip_con_name.table.truncate()
    ingester: Ingester = pip_con_name.logic

    # Assert complex dated Excel ingestion
    assert ingester.raw_file_type.value == RawFileType.EXCEL.value
    assert ingester.directory_type.value == DirectoryType.DATED.value
    assert not ingester.simple_payload

    # Retrieve the DataFrames and ensure type
    dfs: dict[str, DataFrame] = ingester.get_dfs()
    assert isinstance(dfs, dict)

    # Retrieve the DataFrame and ensure it is as expected
    df: DataFrame = dfs[ingester.table.id]
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 3
    assert 'id' in df.columns
    assert 'name' in df.columns
