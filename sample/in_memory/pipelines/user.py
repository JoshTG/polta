from polta.pipeline import PoltaPipeline
from sample.in_memory.canonical.user import \
  pipe as pp_can_user
from sample.in_memory.conformed.activity import \
  pipe as pp_con_activity
from sample.in_memory.conformed.name import \
  pipe as pp_con_name
from sample.in_memory.export.user import \
  pipe as pp_exp_user
from sample.in_memory.raw.activity import \
  pipe as pp_raw_activity


pipeline: PoltaPipeline = PoltaPipeline(
  raw_pipes=[pp_raw_activity],
  conformed_pipes=[pp_con_activity, pp_con_name],
  canonical_pipes=[pp_can_user],
  export_pipes=[pp_exp_user]
)
