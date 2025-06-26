from deltalake import Field, Schema

from polta.enums import (
  DirectoryType,
  WriteLogic,
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
  name='salary',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string'),
    Field('salary', 'long')
  ]),
  metastore=metastore
)

ingester: Ingester = Ingester(
  table=table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.EXCEL,
  write_logic=WriteLogic.APPEND
)

pipe: Pipe = Pipe(ingester)
