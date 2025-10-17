from polta.enums import TableQuality
from polta.pipe import Pipe
from polta.table import Table
from polta.upserter import Upserter
from sample.metastore import metastore
from sample.standard.conformed.state import table as tab_con_state


table: Table = Table(
  domain=tab_con_state.domain,
  quality=TableQuality.CANONICAL,
  name=tab_con_state.name,
  raw_schema=tab_con_state.raw_schema,
  primary_keys=['id'],
  metastore=metastore
)

upserter: Upserter = Upserter(
  source_table=tab_con_state,
  table=table
)

pipe: Pipe = Pipe(upserter)
