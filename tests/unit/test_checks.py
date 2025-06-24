from polars import col, DataFrame
from unittest import TestCase

from tests.unit.testing_data.checks import TestingData


class TestChecks(TestCase):
  """Tests the files in the checks folder"""
  td: TestingData = TestingData()

  def test_check_not_null_or_empty(self) -> None:
    # Run test and collect failed records
    df: DataFrame = (self.td.
      not_null_test.run(self.td.input_df)
      .filter(col(self.td.not_null_result_column) == 0)
    )

    # Assert results are as expected
    assert df.to_dicts() == self.td.not_null_failed_df.to_dicts()
  
  def test_check_positive_int(self) -> None:
    # Run test and collect failed records
    df: DataFrame = (self.td.
      positive_int_test.run(self.td.input_df)
      .filter(col(self.td.positive_int_result_column) == 0)
    )

    # Assert results are as expected
    assert df.to_dicts() == self.td.positive_int_failed_df.to_dicts()
  
  def test_check_value_in(self) -> None:
    # Run test and collect failed records
    df: DataFrame = (self.td.
      value_in_test.run(self.td.input_df)
      .filter(col(self.td.value_in_result_column) == 0)
    )

    # Assert results are as expected
    assert df.to_dicts() == self.td.value_in_df.to_dicts()
