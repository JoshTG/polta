from polars import DataFrame
from unittest import TestCase

from tests.testing_tables.raw.activity import activity_ingest
from tests.testing_tables.conformed.name import name_ingest


class TestIngest(TestCase):
  def test_simple_ingest(self) -> None:
    assert activity_ingest.simple_payload
    df: DataFrame = activity_ingest.ingest()
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
    assert not name_ingest.simple_payload
    df: DataFrame = name_ingest.ingest()
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