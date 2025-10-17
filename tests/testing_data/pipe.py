from datetime import datetime, UTC
from deltalake import Field, Schema
from polars import DataFrame
from typing import Any

from polta.checks import (
  check_not_null_or_empty,
  check_positive_int,
  check_value_in
)
from polta.test import Test
from polta.enums import (
  CheckAction,
  DirectoryType,
  RawFileType,
  TableQuality,
  WriteLogic
)
from polta.ingester import Ingester
from polta.pipe import Pipe
from polta.table import Table
from polta.transformer import Transformer
from polta.upserter import Upserter
from sample.metastore import metastore
from sample.standard.conformed.state import \
  table as tab_con_state


class TestingData:
  """Contains test data for the pipe"""
  table: Table = Table(
    domain='standard',
    quality=TableQuality.RAW,
    name='activity',
    raw_schema=Schema([
      Field('payload', 'string')
    ]),
    metastore=metastore
  )
  overwrite_ingester: Ingester = Ingester(
    table=table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=RawFileType.JSON,
    write_logic=WriteLogic.OVERWRITE
  )
  overwrite_pipe: Pipe = Pipe(overwrite_ingester)
  malformed_ingester: Ingester = Ingester(
    table=table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=RawFileType.JSON,
    write_logic=DirectoryType.SHALLOW
  )
  malformed_pipe: Pipe = Pipe(malformed_ingester)

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
  apply_test_df: DataFrame = DataFrame(
    [
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
    ],
    schema=apply_test_table.schema.polars
  )
  extra_upsert_initial_state: list[str] = ['PA']
  extra_upsert_final_state: list[str] = ['TX']
  extra_upsert_df: DataFrame = DataFrame(
    [
      {
        '_raw_id': '90efe12d-d6d6-4561-840e-9265b873a5d2',
        '_conformed_id': 'df8aad71-fe13-4799-8cdd-ce72e6aed098',
        '_conformed_ts': datetime.now(UTC),
        '_ingested_ts': datetime.now(UTC),
        '_file_path': 'path\\to\\state_2.csv',
        '_file_name': 'state_2.csv',
        '_file_mod_ts': datetime.now(UTC),
        'id': 1,
        'state': 'TX'
      }
    ]
  )
  def apply_test_load_logic() -> dict[str, DataFrame]:
    return {}
  def apply_test_transform_logic(dfs: dict[str, DataFrame]) -> DataFrame:
    return dfs[TestingData().apply_test_table.id]

  apply_test_transformer: Transformer = Transformer(
    apply_test_table,
    load_logic=apply_test_load_logic,
    transform_logic=apply_test_transform_logic,
    write_logic=WriteLogic.APPEND
  )
  apply_test_pipe: Pipe = Pipe(apply_test_transformer)
  apply_test_passed_ids: list[int] = [2]
  apply_test_failed_records: list[dict[str, Any]] = [
    {'id': -1, 'failed_test': 'positive_int'},
    {'id': 3, 'failed_test': 'not_null_or_empty'},
    {'id': 5, 'failed_test': 'not_null_or_empty'}
  ]
  apply_test_quarantined_records: list[dict[str, Any]] = [
    {'id': 4, 'failed_test': 'value_in'}
  ]
