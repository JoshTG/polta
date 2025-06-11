from datetime import datetime, timedelta
from deltalake import DeltaTable
from os import path, remove
from polars import DataFrame
from typing import Any
from unittest import TestCase

from tests.unit.testing_data.table import TestingData


class TestTable(TestCase):
  td: TestingData = TestingData()
  empty_df: DataFrame = DataFrame([], td.table.schema_polars)
  df_1: DataFrame = DataFrame(td.input_dataset_1)
  df_2: DataFrame = DataFrame(td.input_dataset_2)

  def test_build_merge_predicate(self) -> None:
    # Assert table initialization created the merge predicate as expected
    assert self.td.table.merge_predicate == self.td.expected_merge_predicate

  def test_truncate(self) -> None:
    # Assert some data exists in the table
    self.td.table.append(self.df_1)
    assert self.td.table.get().shape[0] > 0

    # Assert the truncate worked as expected
    self.td.table.truncate()
    assert self.td.table.get().shape[0] == 0

  def test_get_table(self) -> None:
    # Truncate table before testing
    self.td.table.truncate()

    # Assert the DataFrame can be pulled and is an empty DataFrame
    df: DataFrame = self.td.table.get()
    assert isinstance(df, DataFrame)
    assert df.is_empty()
  
  def test_enforce_dataframe(self) -> None:
    # Assert dict object works
    df: DataFrame = self.td.table.enforce_dataframe(self.td.input_dataset_1[0])
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 1
    assert df.to_dicts()[0] == self.td.input_dataset_1[0]

    # Assert list of dict objects works
    df: DataFrame = self.td.table.enforce_dataframe(self.td.input_dataset_1)
    assert isinstance(df, DataFrame)
    assert df.shape[0] == self.td.output_dataset_1_len

    # Assert DataFrame works
    df: DataFrame = self.td.table.enforce_dataframe(self.empty_df)
    assert isinstance(df, DataFrame)
    assert df.is_empty()

  def test_touch_state_file(self) -> None:
    # Remove state file first
    if path.exists(self.td.table.state_file_path):
      remove(self.td.table.state_file_path)

    # Assert touch state file creates a state file
    self.td.table.touch_state_file()
    assert path.exists(self.td.table.state_file_path)

  def test_ingestion_zone_directory(self) -> None:
    # Assert a raw table enforces ingestion volume creation
    self.td.raw_table.get()
    assert path.exists(self.td.raw_table.ingestion_zone_path)

  def test_get_last_modified_datetime(self) -> None:
    # Touch state file and assert it is on the same date as today or yesterday
    # This should only fail if executed exactly before midnight on the first of a new month
    self.td.table.touch_state_file()
    last_modified_datetime: datetime = self.td.table.get_last_modified_datetime()
    now: datetime = datetime.now()
    assert last_modified_datetime.year == now.year
    assert last_modified_datetime.month == now.month
    assert last_modified_datetime.day in [now.day, (now - timedelta(days=1)).day]

  def test_get_last_modified_record(self) -> None:
    # Touch state file and assert if is on the same date as today or yesterday
    # This should only fail if executed exactly before midnight on the first of a new month
    self.td.table.touch_state_file()
    last_modified_record: dict[str, Any] = self.td.table.get_last_modified_record()
    now: datetime = datetime.now()
    assert isinstance(last_modified_record, dict)
    assert last_modified_record['domain'] == self.td.table.domain
    assert last_modified_record['quality'] == self.td.table.quality.value
    assert last_modified_record['table'] == self.td.table.name
    assert last_modified_record['path'] == self.td.table.table_path
    assert 'last_modified_datetime' in last_modified_record
    last_modified_datetime: datetime = last_modified_record['last_modified_datetime']
    assert last_modified_datetime.year == now.year
    assert last_modified_datetime.month == now.month
    assert last_modified_datetime.day in [now.day, (now - timedelta(days=1)).day]

  def test_append(self) -> None:
    # Truncate table before testing
    self.td.table.truncate()

    # Append test data into table
    append_df: DataFrame = DataFrame(self.td.input_dataset_1)
    self.td.table.append(append_df)
    
    # Retrieve the table
    df: DataFrame = self.td.table.get()

    # Test rowcount and ID field
    assert df.shape[0] == self.td.output_dataset_1_len
    assert df.select('id').to_dicts() == self.td.output_dataset_1_ids

    # Clear table after testing
    self.td.table.truncate()

  def test_overwrite(self) -> None:
    # Test overwrite with dataset 1
    self.td.table.overwrite(self.df_1)
    out_1: DataFrame = self.td.table.get()
    assert out_1.shape[0] == self.td.output_dataset_1_len
    assert out_1.select('id').to_dicts() == self.td.output_dataset_1_ids

    # Test overwrite with dataset 2
    self.td.table.overwrite(self.df_2)
    out_2: DataFrame = self.td.table.get()
    assert out_2.shape[0] == self.td.output_dataset_2_len
    assert out_2.select('id').to_dicts() == self.td.output_dataset_2_ids

  def test_merge(self) -> None:
    # Truncate table before testing
    self.td.table.truncate()

    # Upsert dataset 1
    self.td.table.upsert(self.df_1)
    out_1: DataFrame = self.td.table.get()
    assert out_1.shape[0] == self.td.output_dataset_1_len
    assert out_1.select('id').sort('id').to_dicts() == self.td.output_dataset_1_ids

    # Test active_ind state after upsert #1
    active_ind_df_1: DataFrame = self.td.table.get(
      filter_conditions={
        'id': self.td.upsert_1_active_test[0],
        'active_ind': self.td.upsert_1_active_test[1]
      }
    )
    assert active_ind_df_1.shape[0] == 1

    # Upsert dataset 2
    self.td.table.upsert(self.df_2)
    out_2: DataFrame = self.td.table.get()
    assert out_2.shape[0] == self.td.upsert_len
    assert out_2.select('id').sort('id').to_dicts() == self.td.upsert_ids

    # Test active_ind state after upsert #2
    active_ind_df_2: list[dict[str, Any]] = self.td.table.get(
      filter_conditions={
        'id': self.td.upsert_2_active_test[0],
        'active_ind': self.td.upsert_2_active_test[1]
      },
      )
    assert active_ind_df_2.shape[0] == 1

  def test_get_as_delta_table(self) -> None:
    # Assert Delta Table retrieval works
    delta_table: DeltaTable = self.td.table.get_as_delta_table()
    assert isinstance(delta_table, DeltaTable)

  def test_drop(self) -> None:
    # Ensure table exists first
    self.td.table.get()

    # Assert drop worked
    self.td.table.drop()
    #assert not path.exists(self.td.table.table_path)
