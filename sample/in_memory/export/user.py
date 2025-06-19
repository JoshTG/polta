from polta.enums import ExportFormat
from polta.exporter import PoltaExporter
from polta.pipe import PoltaPipe

from sample.in_memory.canonical.user import table


exporter: PoltaExporter = PoltaExporter(
  table=table,
  export_format=ExportFormat.JSON
)

pipe: PoltaPipe = PoltaPipe(exporter)
