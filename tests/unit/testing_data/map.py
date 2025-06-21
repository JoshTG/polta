from deltalake import Field, Schema
from deltalake.schema import ArrayType
from polars.datatypes import (
  Boolean,
  DataType,
  Datetime,
  Int32,
  List,
  String
)

from polta.enums import TableQuality


class TestingData:
  expected_deltalake_schema: Schema = Schema([
    Field('id', 'integer'),
    Field('name', 'string'),
    Field('active_ind', 'boolean'),
    Field('tags', ArrayType('string')),
    Field('active_ts', 'timestamp')
  ])
  expected_polars_schema: dict[str, DataType] = {
    'id': Int32,
    'name': String,
    'active_ind': Boolean,
    'tags': List(String),
    'active_ts': Datetime(time_zone='UTC')
  }
  bad_deltalake_fields: list[str] = ['unknown']
  bad_polars_fields: list[str] = ['stirng', 'spam', 'eggs']

  failure_quality_values: list[tuple[TableQuality, str]] = [
    (TableQuality.RAW, '_raw_id'),
    (TableQuality.CONFORMED, '_conformed_id'),
    (TableQuality.CANONICAL, '_canonicalized_id')
  ]
