from json import dumps
from unittest import TestCase

from polta.serializers import json
from tests.testing_data.serializers import TestingData


class TestSerializers(TestCase):
  """Tests the Polta serializers"""
  td: TestingData = TestingData()

  def test_json(self) -> None:
    # Assert default dumps method fails
    self.assertRaises(TypeError, dumps, self.td.json_good_input)

    # Assert JSON serializer works as expected
    assert dumps(self.td.json_good_input, default=json) == self.td.json_output

    # Assert JSON serializer catches non-serializable data
    with self.assertRaises(TypeError) as te:
      dumps(self.td.json_bad_input, default=json)
    self.assertEqual(te.exception.args[0], self.td.type_error_msg)
