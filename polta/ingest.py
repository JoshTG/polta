import polars as pl

from dataclasses import dataclass, field
from datetime import date, datetime
from deltalake import Field, Schema
from os import listdir, path
from polars import DataFrame
from polars.datatypes import DataType, List, String, Struct
from typing import Union
from uuid import uuid4

from polta.enums import DirectoryType, RawFileType
from polta.exceptions import DirectoryTypeNotRecognized
from polta.maps import PoltaMaps
from polta.table import PoltaTable
from polta.types import RawMetadata
from polta.udfs import file_path_to_json, file_path_to_payload


@dataclass
class PoltaIngest:
  table: PoltaTable
  directory_type: DirectoryType
  raw_file_type: RawFileType
  track_history: bool = field(default_factory=lambda: False)

  raw_polars_schema: dict[str, DataType] = field(init=False)
  payload_field: Field = field(init=False)
  simple_payload: bool = field(init=False)
  metadata_schema: Schema = field(init=False)
  payload_schema: dict[str, DataType] = field(init=False)

  def __post_init__(self) -> None:
    self.raw_polars_schema: dict[str, DataType] = PoltaMaps \
      .deltalake_schema_to_polars_schema(self.table.raw_schema)
    self.payload_field: Field = Field('payload', 'string')
    self.simple_payload: bool = self.table.raw_schema.fields == [self.payload_field]
    self.metadata_schema: Schema = PoltaMaps.QUALITY_TO_METADATA_COLUMNS_MAP['raw']
    self.payload_schema: Schema = PoltaMaps.deltalake_schema_to_polars_schema(
      schema=Schema(self.metadata_schema + [self.payload_field])
    )

  def ingest(self, start_date: Union[date, None] = None,
             end_date: Union[date, None] = None) -> DataFrame:
    file_paths: list[str] = self._get_file_paths(start_date, end_date)
    metadata: list[RawMetadata] = [self._get_file_metadata(p) for p in file_paths]
    df: DataFrame = DataFrame(metadata, schema=self.payload_schema)

    if self.track_history:
      df = self._filter_by_history(df)

    return self._ingest_files(df)

  def _get_file_paths(self, start_date: Union[date, None] = None,
                      end_date: Union[date, None] = None) -> list[str]:
    """Retrieves a list of file paths based on ingestion parameters
    
    Args:
      start_date (Union[date, None]): if applicable, the start date for ingestion
      end_date (Union[date, None]): if applicable, the end date for ingestion
    
    Returns:
      file_paths (list[str]): the resulting applicable file paths
    """
    if self.directory_type.value == DirectoryType.SHALLOW.value:
      return [
        path.join(self.table.ingestion_zone_path, f)
        for f in listdir(self.table.ingestion_zone_path)
      ]
    elif self.directory_type.value == DirectoryType.DATED.value:
      for date_str in listdir(self.table.ingestion_zone_path):
        date_obj: date = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        if date_obj < start_date or date_obj > end_date:
          continue
        return [
          path.join(self.table.ingestion_zone_path, date_str, f)
          for f in listdir(self.table.ingestion_zone_path)
        ]
    else:
      raise DirectoryTypeNotRecognized(self.directory_type)

  def _filter_by_history(self, file_paths: list[RawMetadata]) -> list[str]:
    paths: DataFrame = DataFrame(file_paths, schema=self.metadata_schema)
    hx: DataFrame = self.table.get(select=['_file_path', '_file_mod_ts'], unique=True)

    return (paths
      .join(
        other=hx,
        on=[pl.sql_expr('paths._file_path = hx._file_path AND paths._file_mod_ts <= hx._file_mod_ts')],
        how='anti'
      )
    )

  def _ingest_files(self, df: DataFrame) -> DataFrame:
    if self.simple_payload:
      return self._run_simple_load(df)
    elif self.raw_file_type.value == RawFileType.JSON.value:
      return self._run_json_load(df)
    else:
      raise NotImplementedError(self.raw_file_type)

  def _get_file_metadata(self, file_path: str) -> RawMetadata:
    if not path.exists(file_path):
      raise FileNotFoundError()

    return RawMetadata(
      _raw_id=str(uuid4()),
      _ingested_ts=datetime.now(),
      _file_path=file_path,
      _file_name=path.basename(file_path),
      _file_mod_ts=datetime.fromtimestamp(path.getmtime(file_path))
    )

  def _run_simple_load(self, df: DataFrame) -> DataFrame:
    return (df
      .with_columns([
        pl.col('_file_path')
          .map_elements(file_path_to_payload, return_dtype=String)
          .alias('payload')
      ])
    )

  def _run_json_load(self, df: DataFrame) -> DataFrame:
    df: DataFrame = (df
      .with_columns([
        pl.col('_file_path')
          .map_elements(file_path_to_json, return_dtype=List(Struct(self.raw_polars_schema)))
          .alias('payload')
      ])
      .explode('payload')
      .with_columns([
        pl.col('payload').struct.field(f).alias(f)
        for f in self.raw_polars_schema.keys()
      ])
      .drop('payload')
    )
    return df
