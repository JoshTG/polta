from os import getcwd, path

from polta.enums import (
  DirectoryType,
  LoadLogic,
  RawFileType,
  TableQuality
)
from polta.ingest import PoltaIngest
from polta.pipe import PoltaPipe
from polta.table import PoltaTable


activity_table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.RAW,
  name='activity',
  raw_schema=Schema([
    Field('payload', 'string')
  ]),
  metastore_directory=path.join(getcwd(), 'tests', 'testing_tables', 'test_metastore')
)

activity_pipe: PoltaPipe = PoltaPipe(
  table=activity_table,
  load_logic=LoadLogic.APPEND,
  ingest_logic=PoltaIngest(
    table=activity_table,
    directory_type=DirectoryType.SHALLOW,
    raw_file_type=RawFileType.JSON
  )
)
