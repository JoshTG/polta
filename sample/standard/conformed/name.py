from deltalake import Field, Schema

from polta.enums import (
  DirectoryType,
  RawFileType,
  TableQuality
)
from polta.ingester import Ingester
from polta.pipe import Pipe
from polta.table import Table
from sample.metastore import metastore


table: Table = Table(
  domain='standard',
  quality=TableQuality.CONFORMED,
  name='name',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string')
  ]),
  metastore=metastore
)

ingester: Ingester = Ingester(
  table=table,
  directory_type=DirectoryType.DATED,
  raw_file_type=RawFileType.JSON
)

pipe: Pipe = Pipe(ingester)
