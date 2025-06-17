from deltalake import Field, Schema

from polta.enums import (
  DirectoryType,
  RawFileType,
  TableQuality
)
from polta.ingester import PoltaIngester
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from sample.metastore import sample_metastore


table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CONFORMED,
  name='name',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string')
  ]),
  metastore=sample_metastore
)

ingester: PoltaIngester = PoltaIngester(
  table=table,
  directory_type=DirectoryType.DATED,
  raw_file_type=RawFileType.JSON
)

pipe: PoltaPipe = PoltaPipe(ingester)
