from deltalake import Field
from json import loads
from polars.datatypes import DataType
from unittest import TestCase

from polta.exceptions import DataTypeNotRecognized
from polta.maps import Maps
from tests.testing_data.map import TestingData


class TestMaps(TestCase):
  """Tests the Maps class"""
  polta_map: Maps = Maps()
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
    
    # Assert deltalake field mapper fails as expected
    self.assertRaises(TypeError, self.polta_map.deltalake_field_to_polars_field, 4)

    # Assert bad fields get the proper exception raised
    for bad_str in self.td.bad_polars_fields:
      self.assertRaises(DataTypeNotRecognized, Maps.polars_field_to_deltalake_field, bad_str, bad_str)
    for bad_str in self.td.bad_deltalake_fields:
      self.assertRaises(DataTypeNotRecognized, Maps.deltalake_field_to_polars_field, bad_str)

  def test_deltalake_schema_to_polars_schema(self) -> None:
    # Assert a deltalake schema gets mapped properly
    assert self.td.expected_polars_schema == \
      Maps.deltalake_schema_to_polars_schema(self.td.expected_deltalake_schema)

  def test_polars_schema_to_deltalake_schema(self) -> None:
    # Assert a polars schema gets mapped properly
    assert self.td.expected_deltalake_schema == \
      Maps.polars_schema_to_deltalake_schema(self.td.expected_polars_schema)

  def test_quality_to_failure_column(self) -> None:
    # Assert the qualities are mapped to failure columns as expected
    for quality, expected_column in self.td.failure_quality_values:
      assert Maps.quality_to_failure_column(quality) == expected_column
