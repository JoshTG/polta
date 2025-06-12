from deltalake import Field, Schema
from os import getcwd, path

from polta.enums import TableQuality
from polta.table import PoltaTable
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
  metastore_directory=path.join(getcwd(), 'sample', 'test_metastore')
)

pipe: UserPipe = UserPipe(table)
