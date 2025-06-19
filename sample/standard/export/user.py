from polta.enums import ExportFormat
from polta.exporter import Exporter
from polta.pipe import Pipe

from sample.standard.canonical.user import table


exporter: Exporter = Exporter(
  table=table,
  export_format=ExportFormat.CSV
)

pipe: Pipe = Pipe(exporter)
