from deltalake import Field, Schema

from polta.enums import (
  DirectoryType,
  RawFileType,
  TableQuality,
  WriteLogic
)
from polta.ingester import Ingester
from polta.pipe import Pipe
from polta.table import Table
from sample.metastore import metastore


class TestingData:
  table: Table = Table(
    domain='standard',
    quality=TableQuality.RAW,
    name='activity',
    raw_schema=Schema([
      Field('payload', 'string')
    ]),
    metastore=metastore
  )
  overwrite_ingester: Ingester = Ingester(
    table=table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=RawFileType.JSON,
    write_logic=WriteLogic.OVERWRITE
  )
  overwrite_pipe: Pipe = Pipe(overwrite_ingester)
  malformed_ingester: Ingester = Ingester(
    table=table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=RawFileType.JSON,
    write_logic=DirectoryType.SHALLOW
  )
  malformed_pipe: Pipe = Pipe(malformed_ingester)
