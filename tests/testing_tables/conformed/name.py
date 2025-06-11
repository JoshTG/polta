from deltalake import Field, Schema
from os import getcwd, path

from polta.enums import DirectoryType, RawFileType, TableQuality
from polta.ingest import PoltaIngest
from polta.table import PoltaTable


name_table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CONFORMED,
  name='name',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('name', 'string')
  ]),
  primary_keys=['id'],
  metastore_directory=path.join(getcwd(), 'tests', 'unit', 'testing_data', 'test_metastore')
)

name_ingest: PoltaIngest = PoltaIngest(
  table=name_table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.JSON
)
