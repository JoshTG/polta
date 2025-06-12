from polars import DataFrame
from unittest import TestCase

from polta.enums import DirectoryType
from polta.ingester import PoltaIngester
from sample.raw.activity import \
  pipe as pp_raw_activity
from sample.conformed.name import \
  pipe as pp_con_name


class TestIngest(TestCase):
  def test_simple_ingest(self) -> None:
    ingester: PoltaIngester = pp_raw_activity.ingester
    ingester.table.truncate()
    assert ingester.simple_payload
    df: DataFrame = ingester.ingest()
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
    pp_con_name.table.truncate()
    ingester: PoltaIngester = pp_con_name.ingester
    assert ingester.directory_type.value == DirectoryType.DATED.value
    assert not ingester.simple_payload
    df: DataFrame = ingester.ingest()
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
