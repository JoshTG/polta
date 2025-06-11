from deltalake import Field, Schema
from deltalake.schema import ArrayType
from polars.datatypes import (
  Boolean,
  DataType,
  Date,
  Datetime,
  Float32,
  Float64,
  Int32,
  Int64,
  List,
  String
)


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
    'active_ts': Datetime
  }
  bad_polars_fields: list[str] = ['stirng', 'spam', 'eggs']