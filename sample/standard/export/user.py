from polta.enums import ExportFormat
from polta.exporter import PoltaExporter
from polta.pipe import PoltaPipe

from sample.standard.canonical.user import table


exporter: PoltaExporter = PoltaExporter(
  table=table,
  export_format=ExportFormat.CSV
)

pipe: PoltaPipe = PoltaPipe(exporter)
