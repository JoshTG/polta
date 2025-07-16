from datetime import date, datetime, UTC
from typing import Any


class TestingData:
  json_good_input: list[dict[str, Any]] = [
    {
      'id': 1,
      'active_date': date(2025, 1, 1),
      'created_ts': datetime(2025, 2, 1, 12, 30, 00, tzinfo=UTC)
    }
  ]
  json_bad_input: list[dict[str, Any]] = [
    {
      'id': 1,
      'active_date': Any,
      'created_ts': datetime(2025, 2, 1, 12, 30, 00, tzinfo=UTC)
    }
  ]
  json_output: str = '[{"id": 1, "active_date": "2025-01-01", "created_ts": "2025-02-01T12:30:00+00:00"}]'
  type_error_msg: str = f'Error: type <class \'typing._AnyMeta\'> is not serializable'
