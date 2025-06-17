from polta.enums import ExportFormat
from polta.exporter import PoltaExporter

from sample.canonical.user import table


exporter: PoltaExporter = PoltaExporter(
  table=table,
  export_format=ExportFormat.CSV
)
