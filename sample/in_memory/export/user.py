from polta.enums import ExportFormat
from polta.exporter import Exporter
from polta.pipe import Pipe

from sample.in_memory.canonical.user import table


exporter: Exporter = Exporter(
  table=table,
  export_format=ExportFormat.JSON
)

pipe: Pipe = Pipe(exporter)
