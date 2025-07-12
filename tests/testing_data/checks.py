from polars import DataFrame

from polta.checks import *
from polta.test import Test
from polta.enums import CheckAction


class TestingData:
  input_df: DataFrame = DataFrame([
    {'id': 1, 'name': 'Spongebob', 'category': 'FT', 'salary': 55_000},
    {'id': 2, 'name': 'Gary', 'category': '', 'salary': 44_345},
    {'id': 3, 'name': 'Patrick', 'category': 'FT', 'salary': -100_000},
    {'id': 4, 'name': 'Plankton', 'category': 'TP', 'salary': 70_000},
    {'id': 5, 'name': 'Eugene', 'category': 'FT', 'salary': 120_000},
  ])
  
  positive_int_test: Test = Test(
    check=check_positive_int,
    column='salary',
    check_action=CheckAction.FAIL
  )
  positive_int_result_column: str = '__salary__positive_int__'
  positive_int_failed_df: DataFrame = DataFrame([
    {'id': 3, 'name': 'Patrick', 'category': 'FT', 'salary': -100_000, positive_int_result_column: 0},
  ])

  not_null_test: Test = Test(
    check=check_not_null_or_empty,
    column='category',
    check_action=CheckAction.FAIL
  )
  not_null_result_column: str = '__category__not_null_or_empty__'
  not_null_failed_df: DataFrame = DataFrame([
    {'id': 2, 'name': 'Gary', 'category': '', 'salary': 44_345, not_null_result_column: 0}
  ])

  value_in_test: Test = Test(
    check=check_value_in,
    column='category',
    check_action=CheckAction.FAIL,
    kwargs={'values': ['TP']}
  )
  value_in_result_column: str = '__category__value_in__'
  value_in_df: DataFrame = DataFrame([
    {'id': 4, 'name': 'Plankton', 'category': 'TP', 'salary': 70_000, value_in_result_column: 0},
  ])
