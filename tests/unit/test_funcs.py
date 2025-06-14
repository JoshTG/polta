import polars as pl

from unittest import TestCase

from polta.funcs import dedupe
from tests.unit.testing_data.funcs import TestingData

class TestFuncs(TestCase):
  td: TestingData = TestingData()

  def test_dedupe(self) -> None:
    # Assert dedupe logic works as expected
    df: pl.DataFrame = dedupe(
      df=self.td.df,
      keys='id',
      order_by='updated_ts',
      descending=True
    )
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == self.td.updated_rowcount
    updated_name: dict[str, str] = (df
      .filter(pl.col('id') == self.td.test_id)
      .select('name')
      .to_dicts()
      [0]
    )
    assert updated_name == {'name': self.td.updated_name}
