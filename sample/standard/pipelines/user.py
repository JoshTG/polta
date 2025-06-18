from polta.pipeline import PoltaPipeline
from sample.standard.canonical.user import \
  pipe as pp_can_user
from sample.standard.conformed.activity import \
  pipe as pp_con_activity
from sample.standard.conformed.name import \
  pipe as pp_con_name
from sample.standard.export.user import \
  pipe as pp_exp_user
from sample.standard.raw.activity import \
  pipe as pp_raw_activity


pipeline: PoltaPipeline = PoltaPipeline(
  raw_pipes=[pp_raw_activity],
  conformed_pipes=[pp_con_activity, pp_con_name],
  canonical_pipes=[pp_can_user],
  export_pipes=[pp_exp_user]
)
