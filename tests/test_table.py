from deltalake import DeltaTable, Field, Schema
from os import path
from polars import DataFrame, read_delta
from shutil import rmtree
from typing import Any
from unittest import TestCase

from polta.exceptions import PoltaDataFormatNotRecognized
from polta.table import Table
from tests.testing_data.table import TestingData


class TestTable(TestCase):
  """Tests Table class"""
  td: TestingData = TestingData()
  empty_df: DataFrame = DataFrame([], td.table.schema.polars)
  df_1: DataFrame = DataFrame(td.input_dataset_1)
  df_2: DataFrame = DataFrame(td.input_dataset_2)
  apply_test_df: DataFrame = DataFrame(td.apply_test_dataset, td.apply_test_table.schema.polars)
  quarantine_df: DataFrame = DataFrame(td.quarantine_dataset, td.table.schema.quarantine)

  def test_create_if_not_exists(self) -> None:
    # Assert method validates input types correctly
    self.assertRaises(TypeError, Table.create_if_not_exists, 2, Schema([]))
    self.assertRaises(TypeError, Table.create_if_not_exists, 'path', 2)
    self.assertRaises(TypeError, Table.create_if_not_exists, 'path', Schema([]), 2)
    self.assertRaises(TypeError, Table.create_if_not_exists, 'path', Schema([]), [2])
    self.assertRaises(ValueError, Table.create_if_not_exists, 'path', Schema([Field('id', 'string')]), ['name'])

    # Create table and ensure it exists
    Table.create_if_not_exists(
      table_path=self.td.test_path,
      schema=self.td.table.schema.deltalake,
      partition_keys=self.td.table.partition_keys
    )
    assert path.exists(self.td.test_path)
    assert DeltaTable.is_deltatable(self.td.test_path)
    assert DeltaTable(self.td.test_path).metadata().partition_columns == ['active_ind']

    # Clear table if it exists
    if path.exists(self.td.test_path):
      rmtree(self.td.test_path)
    assert not path.exists(self.td.test_path)

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

    # Check filter_conditions validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(filter_conditions=4)
    self.assertEqual(te.exception.args[0], self.td.filter_conditions_msg)
    # Check partition_by validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(partition_by=4)
    self.assertEqual(te.exception.args[0], self.td.partition_by_msg)
    # Check order_by validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(order_by=4)
    self.assertEqual(te.exception.args[0], self.td.order_by_msg)
    # Check order_by_descending validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(order_by_descending=4)
    self.assertEqual(te.exception.args[0], self.td.order_by_descending_msg)
    # Check select validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(select=4)
    self.assertEqual(te.exception.args[0], self.td.select_msg)
    # Check sort_by validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(sort_by=4)
    self.assertEqual(te.exception.args[0], self.td.sort_by_msg)
    # Check limit validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(limit='4')
    self.assertEqual(te.exception.args[0], self.td.limit_msg)
    # Check unique validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(unique=4)
    self.assertEqual(te.exception.args[0], self.td.unique_msg)
    # Check partition_by item validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(partition_by=['id', 4])
    self.assertEqual(te.exception.args[0], self.td.partition_by_item_msg)
    # Check order_by item validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(order_by=['id', 4])
    self.assertEqual(te.exception.args[0], self.td.order_by_item_msg)
    # Check select item validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(select=['id', 4])
    self.assertEqual(te.exception.args[0], self.td.select_item_msg)
    # Check sort_by item validation
    with self.assertRaises(TypeError) as te:
      self.td.table.get(sort_by=['id', 4])
    self.assertEqual(te.exception.args[0], self.td.sort_by_item_msg)

    # Assert table gets created if it does not exist
    self.td.table.drop()
    assert not DeltaTable.is_deltatable(self.td.table.table_path)
    self.td.table.get()
    assert DeltaTable.is_deltatable(self.td.table.table_path)

    # Assert deduplication feature works
    df: DataFrame = self.td.table.get(partition_by=['id'], order_by=['name'], order_by_descending=False)
    assert isinstance(df, DataFrame)

    # Assert limit feature works
    df: DataFrame = self.td.table.get(limit=4)
    assert isinstance(df, DataFrame)
    assert df.shape[0] < 5

    # Assert sort_by feature works
    df: DataFrame = self.td.table.get(sort_by=['id'])
    assert isinstance(df, DataFrame)

  def test_enforce_dataframe(self) -> None:
    # Assert bad data fails
    self.assertRaises(PoltaDataFormatNotRecognized, self.td.table.enforce_dataframe, 4)

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

  def test_ingestion_zone_directory(self) -> None:
    # Remove ingestion zone path first
    rmtree(self.td.raw_table.ingestion_zone_path)

    # Assert a raw table enforces ingestion volume creation
    self.td.raw_table._build_ingestion_zone_if_not_exists()
    assert path.exists(self.td.raw_table.ingestion_zone_path)

  def test_apply_tests(self) -> None:
    # Apply tests against an input dataset
    passed, failed, quarantined = self.td.apply_test_table.apply_tests(self.apply_test_df)

    # Assert IDs are correct for each result DataFrame
    assert sorted([r['id'] for r in passed.to_dicts()]) == self.td.passed_ids
    assert sorted([r['id'] for r in failed.to_dicts()]) == self.td.failed_ids
    assert sorted([r['id'] for r in quarantined.to_dicts()]) == self.td.quarantine_ids

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
    assert df.select('id').sort('id').to_dicts() \
      == self.td.output_dataset_1_ids

    # Clear table after testing
    self.td.table.truncate()

  def test_overwrite(self) -> None:
    # Test overwrite with dataset 1
    self.td.table.overwrite(self.df_1)
    out_1: DataFrame = self.td.table.get()
    assert out_1.shape[0] == self.td.output_dataset_1_len
    assert out_1.select('id').sort('id').to_dicts() \
      == self.td.output_dataset_1_ids

    # Test overwrite with dataset 2
    self.td.table.overwrite(self.df_2)
    out_2: DataFrame = self.td.table.get()
    assert out_2.shape[0] == self.td.output_dataset_2_len
    assert out_2.select('id').sort('id').to_dicts() \
      == self.td.output_dataset_2_ids

  def test_merge(self) -> None:
    # Truncate table before testing
    self.td.table.truncate()

    # Upsert dataset 1
    self.td.table.upsert(self.df_1)
    out_1: DataFrame = self.td.table.get()
    assert out_1.shape[0] == self.td.output_dataset_1_len
    assert out_1.select('id').sort('id').to_dicts() \
      == self.td.output_dataset_1_ids

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
    assert out_2.select('id').sort('id').to_dicts() \
      == self.td.upsert_ids

    # Test active_ind state after upsert #2
    active_ind_df_2: list[dict[str, Any]] = self.td.table.get(
      filter_conditions={
        'id': self.td.upsert_2_active_test[0],
        'active_ind': self.td.upsert_2_active_test[1]
      },
      )
    assert active_ind_df_2.shape[0] == 1

    # Assert validation checks work as expected
    self.assertRaises(ValueError, self.td.raw_table.upsert, self.df_1)
    
  def test_get_as_delta_table(self) -> None:
    # Assert Delta Table retrieval works
    delta_table: DeltaTable = self.td.table.get_as_delta_table()
    assert isinstance(delta_table, DeltaTable)

  def test_drop(self) -> None:
    # Ensure table exists first
    self.td.table.get()

    # Assert drop worked
    self.td.table.drop()

  def test_clear_quarantine(self) -> None:
    # Pre-assertion setup
    self.td.table.clear_quarantine()

    # Add one record to the quarantine and ensure it loaded
    self.quarantine_df.write_delta(
      target=self.td.table.quarantine_path,
      mode='append'
    )
    df: DataFrame = read_delta(self.td.table.quarantine_path)
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 1

    # Clear quarantine and ensure the record got deleted
    self.td.table.clear_quarantine()
    df: DataFrame = read_delta(self.td.table.quarantine_path)
    assert isinstance(df, DataFrame)
    assert df.shape[0] == 0

  def test_build_ingestion_zone(self) -> None:
    # Run ingestion zone method
    self.td.table._build_ingestion_zone_if_not_exists()
    assert path.exists(self.td.table.ingestion_zone_path)

  def test_standard_table(self) -> None:
    # Retrieve the standard table as a DataFrame
    df: DataFrame = self.td.standard_table.get()
    df: DataFrame = df.drop('_id')
    df: DataFrame = self.td.standard_table._preprocess(df)

    # Ensure it matches the expected type and columns
    assert isinstance(df, DataFrame)
    assert list(df.schema.keys()) \
      == self.td.standard_table.schema.columns
