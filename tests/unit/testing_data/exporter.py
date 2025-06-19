from polta.enums import ExportFormat
from polta.exporter import PoltaExporter
from polta.pipe import PoltaPipe

from polta.enums import DirectoryType
from sample.standard.canonical.user import table


class TestingData:
  exporter: PoltaExporter = PoltaExporter(
    table=table,
    export_format=ExportFormat.CSV
  )
  pipe: PoltaPipe = PoltaPipe(exporter)

  malformed_exporter: PoltaExporter = PoltaExporter(
    table=table,
    export_format=DirectoryType.SHALLOW
  )
  malformed_pipe: PoltaPipe = PoltaPipe(malformed_exporter)
