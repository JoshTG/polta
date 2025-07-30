from polta.pipeline import Pipeline
from sample.standard.canonical.user import \
  pipe as pip_can_user
from sample.standard.conformed.activity import \
  pipe as pip_con_activity
from sample.standard.conformed.name import \
  pipe as pip_con_name
from sample.standard.export.user import \
  pipe as pip_exp_user
from sample.standard.raw.activity import \
  pipe as pip_raw_activity
from sample.standard.standard.category import \
  pipe as pip_std_category


pipeline: Pipeline = Pipeline(
  raw_pipes=[pip_raw_activity],
  conformed_pipes=[pip_con_activity, pip_con_name],
  standard_pipes=[pip_std_category],
  canonical_pipes=[pip_can_user],
  export_pipes=[pip_exp_user]
)
