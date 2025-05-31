from deltalake import Field, Schema
from os import getcwd, path
from typing import Any

from polta.enums import TableQuality
from polta.table import PoltaTable


class TestingData:
  table: PoltaTable = PoltaTable(
    domain='tests',
    quality=TableQuality.CANONICAL,
    name='test_table',
    raw_schema=Schema([
      Field('id', 'integer'),
      Field('name', 'string'),
      Field('active_ind', 'boolean')
    ]),
    primary_keys=['id', 'name'],
    partition_keys=['id'],
    metastore_directory=path.join(getcwd(), 'tests', 'testing_data', 'test_metastore'),
    delta_configuration={
      'delta.enableChangeDataFeed': 'true'
    },
    delta_metadata={
      'test-table': 'true'
    }
  )

  expected_merge_predicate: str = 's.id = t.id AND s.name = t.name'

  input_dataset_1: list[dict[str, Any]] = [
    {'id': 1, 'name': 'Spongebob Squarepants', 'active_ind': True},
    {'id': 2, 'name': 'Gary the Snail', 'active_ind': False},
    {'id': 3, 'name': 'Plankton', 'active_ind': False}
  ]
  output_dataset_1_ids: list[dict[str, int]] = [
    {'id': 1},
    {'id': 2},
    {'id': 3}
  ]
  output_dataset_1_len: int = 3

  input_dataset_2: list[dict[str, Any]] = [
    {'id': 3, 'name': 'Plankton', 'active_ind': True},
    {'id': 4, 'name': 'Mr. Eugene Crabs', 'active_ind': True}
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