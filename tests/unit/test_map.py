from deltalake import Field
from json import loads
from polars.datatypes import DataType
from unittest import TestCase

from polta.exceptions import DataTypeNotRecognized
from polta.maps import PoltaMaps
from tests.unit.testing_data.map import TestingData


class TestMap(TestCase):
  polta_map: PoltaMaps = PoltaMaps()
  td: TestingData = TestingData()

  def test_polars_to_deltalake_map(self) -> None:
    # Assert each polars field maps to a deltalake field
    for i in range(len(self.td.expected_deltalake_schema.fields)):
      expected_dl_field: Field = self.td.expected_deltalake_schema.fields[i]
      pl_col, pl_dt = list(self.td.expected_polars_schema.items())[i]
      dl_field: Field = self.polta_map.polars_field_to_deltalake_field(pl_col, pl_dt)
      assert dl_field == expected_dl_field

  def test_deltalake_to_polars_map(self) -> None:
    # Assert each deltalake fields maps to a polars field
    for i in range(len(self.td.expected_polars_schema.keys())):
      dl_field: str = loads(self.td.expected_deltalake_schema.fields[i].to_json())['type']
      expected_pl_field: DataType = list(self.td.expected_polars_schema.values())[i]
      pl_field: DataType = self.polta_map.deltalake_field_to_polars_field(dl_field)
      assert pl_field == expected_pl_field

    # Assert bad fields get the proper exception raised
    for bad_str in self.td.bad_polars_fields:
      self.assertRaises(DataTypeNotRecognized, PoltaMaps.polars_field_to_deltalake_field, bad_str, bad_str)

  def test_deltalake_schema_to_polars_schema(self) -> None:
    # Assert a deltalake schema gets mapped properly
    assert self.td.expected_polars_schema == \
      PoltaMaps.deltalake_schema_to_polars_schema(self.td.expected_deltalake_schema)

  def test_polars_schema_to_deltalake_schema(self) -> None:
    # Assert a polars schema gets mapped properly
    assert self.td.expected_deltalake_schema == \
      PoltaMaps.polars_schema_to_deltalake_schema(self.td.expected_polars_schema)
