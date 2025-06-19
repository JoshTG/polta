from deltalake import Field, Schema

from polta.enums import (
  DirectoryType,
  RawFileType,
  TableQuality,
  WriteLogic
)
from polta.ingester import PoltaIngester
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from sample.metastore import metastore


class TestingData:
  table: PoltaTable = PoltaTable(
    domain='standard',
    quality=TableQuality.RAW,
    name='activity',
    raw_schema=Schema([
      Field('payload', 'string')
    ]),
    metastore=metastore
  )
  overwrite_ingester: PoltaIngester = PoltaIngester(
    table=table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=RawFileType.JSON,
    write_logic=WriteLogic.OVERWRITE
  )
  overwrite_pipe: PoltaPipe = PoltaPipe(overwrite_ingester)
  malformed_ingester: PoltaIngester = PoltaIngester(
    table=table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=RawFileType.JSON,
    write_logic=DirectoryType.SHALLOW
  )
  malformed_pipe: PoltaPipe = PoltaPipe(malformed_ingester)
