from polars import DataFrame
from unittest import TestCase

from polta.enums import DirectoryType
from sample.raw.activity import \
  ingester as pin_raw_activity
from sample.conformed.name import \
  ingester as pin_con_name


class TestIngest(TestCase):
  def test_simple_ingest(self) -> None:
    pin_raw_activity.table.truncate()
    assert pin_raw_activity.simple_payload
    df: DataFrame = pin_raw_activity.get_dfs()['source']
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
    pin_con_name.table.truncate()
    assert pin_con_name.directory_type.value == DirectoryType.DATED.value
    assert not pin_con_name.simple_payload
    df: DataFrame = pin_con_name.get_dfs()['source']
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 3
    assert '_raw_id' in df.columns
    assert '_ingested_ts' in df.columns
    assert '_file_path' in df.columns
    assert '_file_name' in df.columns
    assert '_file_mod_ts' in df.columns
    assert 'id' in df.columns
    assert 'name' in df.columns
    assert len(df.columns) == 7
