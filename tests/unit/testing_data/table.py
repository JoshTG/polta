from datetime import datetime
from deltalake import Field, Schema
from os import getcwd, path
from typing import Any

from polta.enums import TableQuality
from polta.table import PoltaTable
from tests.testing_tables.raw.activity import activity_table


class TestingData:
  raw_table: PoltaTable = activity_table
  table: PoltaTable = PoltaTable(
    domain='test',
    quality=TableQuality.CANONICAL,
    name='test_table',
    raw_schema=Schema([
      Field('id', 'integer'),
      Field('name', 'string'),
      Field('active_ind', 'boolean')
    ]),
    primary_keys=['id', 'name'],
    metastore_directory=path.join(getcwd(), 'tests', 'testing_tables', 'test_metastore')
  )

  expected_merge_predicate: str = 's.id = t.id AND s.name = t.name'

  input_dataset_1: list[dict[str, Any]] = [
    {
      '_raw_id': 'abc',
      '_conformed_id': 'def',
      '_canonicalized_id': 'ghi',
      '_created_ts': datetime(2025, 1, 1, 1, 1, 1),
      '_modified_ts': datetime(2025, 1, 1, 1, 1, 1),
      'id': 1,
      'name': 'Spongebob Squarepants',
      'active_ind': True
    },
    {
      '_raw_id': 'jkl',
      '_conformed_id': 'mno',
      '_canonicalized_id': 'pqr',
      '_created_ts': datetime(2025, 1, 1, 1, 1, 1),
      '_modified_ts': datetime(2025, 1, 1, 1, 1, 1),
      'id': 2,
      'name': 'Gary the Snail',
      'active_ind': False
    },
    {
      '_raw_id': 'stu',
      '_conformed_id': 'vwx',
      '_canonicalized_id': 'yza',
      '_created_ts': datetime(2025, 1, 1, 1, 1, 1),
      '_modified_ts': datetime(2025, 1, 1, 1, 1, 1),
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
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
      'id': 3,
      'name': 'Plankton',
      'active_ind': True
    },
    {
      '_raw_id': 'klm',
      '_conformed_id': 'nop',
      '_canonicalized_id': 'qrs',
      '_created_ts': datetime.now(),
      '_modified_ts': datetime.now(),
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

  upsert_1_active_test: tuple[int, bool] = [3, False]
  upsert_2_active_test: tuple[int, bool] = [3, True]
  upsert_ids: list[dict[str, int]] = [
    {'id': 1},
    {'id': 2},
    {'id': 3},
    {'id': 4}
  ]
  upsert_len: int = 4