from deltalake import Field, Schema

from polta.enums import TableQuality
from polta.table import PoltaTable
from sample.metastore import sample_metastore

from .pipes.user import UserPipe


table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CANONICAL,
  name='user',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string'),
    Field('active_ind', 'boolean')
  ]),
  primary_keys=['id'],
  metastore=sample_metastore
)

pipe: UserPipe = UserPipe(table)
