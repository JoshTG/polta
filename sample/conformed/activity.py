from deltalake import Field, Schema

from polta.enums import TableQuality
from polta.table import PoltaTable
from sample.metastore import sample_metastore

from .pipes.activity import ActivityPipe


table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CONFORMED,
  name='activity',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('active_ind', 'boolean')
  ]),
  metastore=sample_metastore
)

pipe: ActivityPipe = ActivityPipe(table)
