from deltalake import Field, Schema
from os import getcwd, path

from polta.enums import TableQuality
from polta.table import Table
from sample.metastore import metastore_init


class TestingData:
  domains: list[str] = ['in_memory', 'standard']
  qualities: list[TableQuality] = [
    TableQuality.CANONICAL,
    TableQuality.CONFORMED,
    TableQuality.RAW
  ]
  domain_existence_checks: list[tuple[str, bool]] = [
    ('standard', True),
    ('in_memory', True),
    ('plankton', False)
  ]
  quality_existence_checks: list[tuple[str, TableQuality, bool]] = [
    ('standard', TableQuality.CANONICAL, True),
    ('standard', TableQuality.CONFORMED, True),
    ('standard', TableQuality.RAW, True),
  ]
  
  test_path: str = path.join(getcwd(), 'sample', 'test_metastore_init', 'volumes', 'test_zone', 'table')
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
    metastore=metastore_init
  )
