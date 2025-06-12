from deltalake import Field, Schema
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


name_table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CONFORMED,
  name='name',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string')
  ]),
  metastore_directory=path.join(getcwd(), 'tests', 'testing_tables', 'test_metastore')
)

name_ingest: PoltaIngest = PoltaIngest(
  table=name_table,
  directory_type=DirectoryType.DATED,
  raw_file_type=RawFileType.JSON
)

name_pipe: PoltaPipe = PoltaPipe(
  table=name_table,
  load_logic=LoadLogic.APPEND,
  ingest_logic=name_ingest
)
