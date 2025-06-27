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
  domain='in_memory',
  quality=TableQuality.RAW,
  name='activity',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('active_ind', 'boolean')
  ]),
  metastore=metastore
)

ingester: Ingester = Ingester(
  table=table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.CSV,
  write_logic=WriteLogic.APPEND
)

pipe: Pipe = Pipe(ingester)
