from polta.pipeline import PoltaPipeline
from sample.in_memory.canonical.user import \
  pipe as pip_can_user
from sample.in_memory.conformed.activity import \
  pipe as pip_con_activity
from sample.in_memory.conformed.name import \
  pipe as pip_con_name
from sample.in_memory.export.user import \
  pipe as pip_exp_user
from sample.in_memory.raw.activity import \
  pipe as pip_raw_activity


pipeline: PoltaPipeline = PoltaPipeline(
  raw_pipes=[pip_raw_activity],
  conformed_pipes=[pip_con_activity, pip_con_name],
  canonical_pipes=[pip_can_user],
  export_pipes=[pip_exp_user]
)
