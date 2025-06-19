from deltalake import Field, Schema

from polta.enums import (
  DirectoryType,
  WriteLogic,
  RawFileType,
  TableQuality
)
from polta.ingester import PoltaIngester
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from sample.metastore import metastore


table: PoltaTable = PoltaTable(
  domain='standard',
  quality=TableQuality.RAW,
  name='activity',
  raw_schema=Schema([
    Field('payload', 'string')
  ]),
  metastore=metastore
)

ingester: PoltaIngester = PoltaIngester(
  table=table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.JSON,
  write_logic=WriteLogic.APPEND
)

pipe: PoltaPipe = PoltaPipe(ingester)
