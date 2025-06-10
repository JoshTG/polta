from deltalake import Field, Schema
from os import getcwd, path

from polta.enums import DirectoryType, RawFileType, TableQuality
from polta.ingest import PoltaIngest
from polta.table import PoltaTable


activity_table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.RAW,
  name='activity',
  raw_schema=Schema([
    Field('payload', 'string')
  ]),
  primary_keys=['id'],
  metastore_directory=path.join(getcwd(), 'tests', 'testing_data', 'test_metastore')
)

activity_ingest: PoltaIngest = PoltaIngest(
  table=activity_table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.JSON
)
