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
  name='profit',
  raw_schema=Schema([
    Field('id', 'integer'),
    Field('profit', 'float')
  ]),
  metastore=metastore
)

ingester: Ingester = Ingester(
  table=table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.CSV
)

pipe: Pipe = Pipe(ingester)
