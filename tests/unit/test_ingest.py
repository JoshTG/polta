from polars import DataFrame
from unittest import TestCase

from polta.enums import DirectoryType
from sample.standard.raw.activity import \
  ingester as in_raw_activity
from sample.standard.conformed.name import \
  ingester as in_con_name


class TestIngest(TestCase):
  """Tests the Ingester class"""
  def test_simple_ingest(self) -> None:
    # Pre-assertion cleanup
    in_raw_activity.table.truncate()

    # Assert simple ingest
    assert in_raw_activity.simple_payload

    # Retrieve the DataFrames and ensure type
    dfs: dict[str, DataFrame] = in_raw_activity.get_dfs()
    assert isinstance(dfs, dict)
    assert list(dfs.keys()) == [in_raw_activity.table.id]

    # Retrieve the DataFrame and ensure it is as expected
    df: DataFrame = dfs[in_raw_activity.table.id]
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
    in_con_name.table.truncate()

    # Assert dated JSON ingest
    assert in_con_name.directory_type.value == DirectoryType.DATED.value
    assert not in_con_name.simple_payload

    # Retrieve the DataFrames and ensure type
    dfs: dict[str, DataFrame] = in_con_name.get_dfs()
    assert isinstance(dfs, dict)

    # Retrieve the DataFrame and ensure it is as expected
    df: DataFrame = dfs[in_con_name.table.id]
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
