from polta.exporter import Exporter
from polta.pipe import Pipe

from polta.enums import DirectoryType
from sample.standard.export.profit import exporter


class TestingData:
  exporter: Exporter = exporter
  pipe: Pipe = Pipe(exporter)

  malformed_exporter: Exporter = Exporter(
    table=exporter.table,
    export_format=DirectoryType.SHALLOW
  )
  malformed_pipe: Pipe = Pipe(malformed_exporter)
