from polta.pipeline import PoltaPipeline
from tests.testing_tables.canonical.user import (
  user_pipe,
)
from tests.testing_tables.conformed.activity import (
  activity_pipe as conformed_activity_pipe
)
from tests.testing_tables.conformed.name import (
  name_pipe
)
from tests.testing_tables.raw.activity import (
  activity_pipe as raw_activity_pipe
)

user_pipeline: PoltaPipeline = PoltaPipeline(
  raw_pipes=[raw_activity_pipe],
  conformed_pipes=[name_pipe, conformed_activity_pipe],
  canonical_pipes=[user_pipe]
)
