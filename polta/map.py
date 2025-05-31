from deltalake import DataType as dlDataType, Field, Schema
from deltalake.schema import ArrayType
from json import loads
from polars.datatypes import (
  Array,
  Boolean,
  DataType as plDataType,
  Date,
  Datetime,
  Float32,
  Float64,
  Int32,
  Int64,
  List,
  String
)
from typing import Union

from polta.exceptions import DataTypeNotRecognized


class PoltaMap:
  DELTALAKE_TO_POLARS_FIELD_MAP: dict[str, plDataType] = {
    'boolean': Boolean,
    'date': Date,
    'double': Float64,
    'float': Float32,
    'integer': Int32,
    'long': Int64,
    'string': String,
    'timestamp': Datetime(time_zone='America/New_York')
  }
  POLARS_TO_DELTALAKE_FIELD_MAP: dict[plDataType, str] = {
    Boolean: 'boolean',
    Date: 'date',
    Float64: 'double',
    Float32: 'float',
    Int32: 'integer',
    Int64: 'long',
    String: 'string',
    Datetime(time_zone='America/New_York'): 'timestamp'
  }

  @staticmethod
  def deltalake_field_to_polars_field(delta_field: Union[dict, str]) -> plDataType:
    """Maps an individual Delta field to a Polars data type
    
    Args:
      delta_field (str): the field name (e.g., 'boolean', 'date')
    
    Returns:
      dt (DataType): the resulting Polars data type
    """
    wrap_in_list: bool = False
    if isinstance(delta_field, dict) and delta_field.get('type') == 'array':
      delta_field: str = delta_field.get('elementType')
      wrap_in_list: bool = True
    if not isinstance(delta_field, str):
      raise TypeError('Error: delta_field must be of type <str> or <dict>')
    dt: Union[plDataType, str] = PoltaMap.DELTALAKE_TO_POLARS_FIELD_MAP.get(delta_field, '')
    if isinstance(dt, str):
      raise DataTypeNotRecognized(dt)
    return List(dt) if wrap_in_list else dt

  @staticmethod
  def deltalake_schema_to_polars_schema(schema: Schema) -> dict[str, plDataType]:
    """Converts the existing Delta Lake schema and returns it as a dict compatible to Polars DataFrames

    Args:
      schema (Schema): the original deltalake schema
    
    Returns:
      schema_polars (dict[str, DataType]): the schema as a dict, compatible with Polars DataFrames
    """
    polars_schema: dict[str, plDataType] = {}

    for field in loads(schema.to_json())['fields']:
      polars_schema[field['name']] = PoltaMap.deltalake_field_to_polars_field(field['type'])
      if field['type'] == 'timestamp':
        polars_schema[field['name']]
      
    return polars_schema  

  @staticmethod
  def polars_field_to_deltalake_field(column: str, data_type: plDataType) -> Field:
    try:
      if isinstance(data_type, Array):
        return Field(column, data_type.element_type)
      elif isinstance(data_type, List):
        dt: Union[str, None] = PoltaMap.POLARS_TO_DELTALAKE_FIELD_MAP[data_type.inner]
        return Field(column, ArrayType(dt))
      else:
        dt: Union[str, None] = PoltaMap.POLARS_TO_DELTALAKE_FIELD_MAP[data_type]
        return Field(column, dt)
    except KeyError:
      raise DataTypeNotRecognized(data_type)
  
  @staticmethod
  def polars_schema_to_deltalake_schema(schema: dict[str, plDataType]) -> Schema:
    """Converts a polars schema to a deltalake schema
    
    Args:
      schema (dict[str, DataType]): the original polars schema
    
    Returns:
      schema (Schema): the resulting deltalake schema
    """
    fields: list[plDataType] = []
    for column, data_type in schema.items():
      fields.append(PoltaMap.polars_field_to_deltalake_field(column, data_type))
    return Schema(fields)
