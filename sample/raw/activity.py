from deltalake import Field, Schema
from os import getcwd, path

from polta.enums import (
  DirectoryType,
  LoadLogic,
  RawFileType,
  TableQuality
)
from polta.ingester import PoltaIngester
from polta.pipe import PoltaPipe
from polta.table import PoltaTable


table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.RAW,
  name='activity',
  raw_schema=Schema([
    Field('payload', 'string')
  ]),
  metastore_directory=path.join(getcwd(), 'sample', 'test_metastore')
)

ingester: PoltaIngester = PoltaIngester(
  table=table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.JSON
)

pipe: PoltaPipe = PoltaPipe(
  table=table,
  load_logic=LoadLogic.APPEND,
  ingester=ingester
)
