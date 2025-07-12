from polta.enums import ExportFormat
from polta.exporter import Exporter
from polta.pipe import Pipe

from polta.enums import DirectoryType
from sample.standard.canonical.user import table


class TestingData:
  exporter: Exporter = Exporter(
    table=table,
    export_format=ExportFormat.CSV
  )
  pipe: Pipe = Pipe(exporter)

  malformed_exporter: Exporter = Exporter(
    table=table,
    export_format=DirectoryType.SHALLOW
  )
  malformed_pipe: Pipe = Pipe(malformed_exporter)
