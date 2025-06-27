from datetime import datetime, UTC
from deltalake import Field, Schema
from os import getcwd, path
from typing import Any

from polta.checks import *
from polta.test import Test
from polta.enums import CheckAction, TableQuality
from polta.table import Table
from polta.table_schema import TableSchema
from sample.metastore import metastore


class TestingData:
  raw_table: Table = Table(
    domain='standard',
    quality=TableQuality.RAW,
    name='activity_test',
    raw_schema=Schema([
      Field('payload', 'string')
    ]),
    metastore=metastore
  )
  table: Table = Table(
    domain='standard',
    quality=TableQuality.CANONICAL,
    name='test_table',
    raw_schema=Schema([
      Field('id', 'integer'),
      Field('name', 'string'),
      Field('active_ind', 'boolean')
    ]),
    primary_keys=['id', 'name'],
    metastore=metastore
  )

  test_path: str = path.join(getcwd(), 'sample', 'test_metastore', 'volumes', 'test_zone', 'table')
  filter_conditions_msg: str = 'Error: filter_conditions must be of type <dict>'
  partition_by_msg: str = 'Error: partition_by must be of type <list>'
  order_by_msg: str = 'Error: order_by must be of type <list>'
  order_by_descending_msg: str = 'Error: order_by_descending must be of type <bool>'
  select_msg: str = 'Error: select must be of type <list>'
  sort_by_msg: str = 'Error: sort_by must be of type <list>'
  limit_msg: str = 'Error: limit must be of type <int>'
  unique_msg: str = 'Error: unique must be of type <bool>'
  partition_by_item_msg: str = 'Error: all values in partition_by must be of type <str>'
  order_by_item_msg: str = 'Error: all values in order_by must be of type <str>'
  select_item_msg: str = 'Error: all values in select must be of type <str>'
  sort_by_item_msg: str = 'Error: all values in sort_by must be of type <str>'

  expected_merge_predicate: str = 's.id = t.id AND s.name = t.name'

  input_dataset_1: list[dict[str, Any]] = [
    {
      '_raw_id': 'abc',
      '_conformed_id': 'def',
      '_canonicalized_id': 'ghi',
      '_created_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      '_modified_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      'id': 1,
      'name': 'Spongebob Squarepants',
      'active_ind': True
    },
    {
      '_raw_id': 'jkl',
      '_conformed_id': 'mno',
      '_canonicalized_id': 'pqr',
      '_created_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      '_modified_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      'id': 2,
      'name': 'Gary the Snail',
      'active_ind': False
    },
    {
      '_raw_id': 'stu',
      '_conformed_id': 'vwx',
      '_canonicalized_id': 'yza',
      '_created_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      '_modified_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      'id': 3,
      'name': 'Plankton',
      'active_ind': False
    }
  ]
  output_dataset_1_ids: list[dict[str, int]] = [
    {'id': 1},
    {'id': 2},
    {'id': 3}
  ]
  output_dataset_1_len: int = 3

  input_dataset_2: list[dict[str, Any]] = [
    {
      '_raw_id': 'bcd',
      '_conformed_id': 'efg',
      '_canonicalized_id': 'hij',
      '_created_ts': datetime.now(UTC),
      '_modified_ts': datetime.now(UTC),
      'id': 3,
      'name': 'Plankton',
      'active_ind': True
    },
    {
      '_raw_id': 'klm',
      '_conformed_id': 'nop',
      '_canonicalized_id': 'qrs',
      '_created_ts': datetime.now(UTC),
      '_modified_ts': datetime.now(UTC),
      'id': 4,
      'name': 'Mr. Eugene Crabs',
      'active_ind': True
    }
  ]
  output_dataset_2_ids: list[dict[str, int]] = [
    {'id': 3},
    {'id': 4}
  ] 
  output_dataset_2_len: int = 2

  quarantine_dataset: list[dict[str, Any]] = [
    {
      '_raw_id': 'abc',
      '_conformed_id': 'def',
      '_canonicalized_id': 'ghi',
      '_created_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      '_modified_ts': datetime(2025, 1, 1, 1, 1, 1, tzinfo=UTC),
      'id': 1,
      'name': 'Spongebob Squarepants',
      'active_ind': True,
      'failed_test': '__test__'
    }
  ]

  upsert_1_active_test: tuple[int, bool] = [3, False]
  upsert_2_active_test: tuple[int, bool] = [3, True]
  upsert_ids: list[dict[str, int]] = [
    {'id': 1},
    {'id': 2},
    {'id': 3},
    {'id': 4}
  ]
  upsert_len: int = 4

  apply_test_table: Table = Table(
    domain='standard',
    quality=TableQuality.CANONICAL,
    name='apply_tests',
    raw_schema=Schema([
      Field('id', 'integer'),
      Field('name', 'string'),
      Field('active_ind', 'boolean'),
      Field('category', 'string')
    ]),
    primary_keys=['id', 'name'],
    metastore=metastore,
    tests=[
      Test(check_not_null_or_empty, 'category', CheckAction.FAIL),
      Test(check_positive_int, 'id', CheckAction.FAIL),
      Test(check_value_in, 'active_ind', CheckAction.QUARANTINE, {'values': [False]})
    ]
  )
  apply_test_dataset: list[dict[str, Any]] = [
    {
      '_raw_id': 'abc',
      '_conformed_id': 'def',
      '_canonicalized_id': 'hij',
      '_created_ts': datetime.now(UTC),
      '_modified_ts': datetime.now(UTC),
      'id': -1,
      'name': 'Spongebob',
      'active_ind': True,
      'category': 'PT'
    },
    {
      '_raw_id': 'klm',
      '_conformed_id': 'nop',
      '_canonicalized_id': 'qrs',
      '_created_ts': datetime.now(UTC),
      '_modified_ts': datetime.now(UTC),
      'id': 2,
      'name': 'Patrick',
      'active_ind': True,
      'category': 'FT'
    },
    {
      '_raw_id': 'tuv',
      '_conformed_id': 'wxy',
      '_canonicalized_id': 'zab',
      '_created_ts': datetime.now(UTC),
      '_modified_ts': datetime.now(UTC),
      'id': 3,
      'name': 'Gary',
      'active_ind': True,
      'category': ''
    },
    {
      '_raw_id': 'cde',
      '_conformed_id': 'fgh',
      '_canonicalized_id': 'ijk',
      '_created_ts': datetime.now(UTC),
      '_modified_ts': datetime.now(UTC),
      'id': 4,
      'name': 'Plankton',
      'active_ind': False,
      'category': 'PT'
    },
    {
      '_raw_id': 'lmn',
      '_conformed_id': 'opq',
      '_canonicalized_id': 'rst',
      '_created_ts': datetime.now(UTC),
      '_modified_ts': datetime.now(UTC),
      'id': 5,
      'name': 'Eugene',
      'active_ind': True,
      'category': None
    }
  ]
  passed_ids: list[int] = [2]
  failed_ids: list[int] = [-1, 3, 5]
  quarantine_ids: list[int] = [4]
