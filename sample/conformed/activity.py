from deltalake import Field, Schema
from os import getcwd, path

from polta.enums import TableQuality
from polta.table import PoltaTable
from .pipes.activity import ActivityPipe


table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CONFORMED,
  name='activity',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('active_ind', 'boolean')
  ]),
  metastore_directory=path.join(getcwd(), 'sample', 'test_metastore')
)

pipe: ActivityPipe = ActivityPipe(table)
