from deltalake import DeltaTable, Schema
from os import path
from polars import DataFrame
from shutil import rmtree
from unittest import TestCase

from polta.enums import TableQuality
from polta.exceptions import DomainDoesNotExist
from polta.metastore import Metastore, pipe_history
from sample.metastore import metastore, metastore_init
from sample.standard.pipelines.user import pip_can_user
from tests.testing_data.metastore import TestingData


class TestMetastore(TestCase):
  """Tests the Metastore class"""
  # Retrieve test data and initialize the metastore
  td: TestingData = TestingData()
  pm: Metastore = metastore
  pm_init: Metastore = metastore_init
  # Execute the pipeline to ensure tables exist
  pip_can_user.execute()

  def test_init(self) -> None:
    # Assert top-level directories were initialized
    assert path.exists(self.pm.main_path)
    assert path.exists(self.pm.tables_directory)
    assert path.exists(self.pm.volumes_directory)

    # Destory secondary metastore
    rmtree(self.pm_init.main_path)
    assert not path.exists(self.pm_init.main_path)
    
    # Assert secondary metastore re-initializes as expected
    self.pm_init.initialize_if_not_exists()
    assert path.exists(self.pm_init.main_path)
    assert path.exists(self.pm_init.tables_directory)
    assert path.exists(self.pm_init.volumes_directory)

  def test_create_table_if_not_exists(self) -> None:
    # Assert method validates input types correctly
    self.assertRaises(TypeError, self.pm_init.create_table_if_not_exists, 2, Schema([]))
    self.assertRaises(TypeError, self.pm_init.create_table_if_not_exists, 'path', 2)
    self.assertRaises(TypeError, self.pm_init.create_table_if_not_exists, 'path', Schema([]), 2)
    self.assertRaises(TypeError, self.pm_init.create_table_if_not_exists, 'path', Schema([]), [2])

    # Create table and ensure it exists
    self.pm_init.create_table_if_not_exists(
      table_path=self.td.test_path,
      schema=self.td.table.schema.deltalake
    )
    assert path.exists(self.td.test_path)
    assert DeltaTable.is_deltatable(self.td.test_path)

    # Create again and ensure it returns
    self.pm_init.create_table_if_not_exists(
      table_path=self.td.test_path,
      schema=self.td.table.schema.deltalake
    )
    assert path.exists(self.td.test_path)
    assert DeltaTable.is_deltatable(self.td.test_path)

    # Post-assertion cleanup
    rmtree(self.td.test_path)

  def test_list_domains(self) -> None:
    # Assert metastore can return the list of domains
    domains: list[str] = self.pm.list_domains()
    assert domains == self.td.domains
  
  def test_list_qualities(self) -> None:
    # Assert metastore can return the list of qualities
    qualities: list[TableQuality] = self.pm.list_qualities(self.td.domains[1])
    assert sorted([q.value for q in qualities]) == \
      sorted([q.value for q in self.td.qualities])
    self.assertRaises(DomainDoesNotExist, self.pm.list_qualities, 'nonexistent')
  
  def test_domain_exists(self) -> None:
    # Assert metastore can determine domain existence correctly
    for domain, expected_result in self.td.domain_existence_checks:
      assert self.pm.domain_exists(domain) == expected_result
  
  def test_quality_exists(self) -> None:
    # Assert metastore can determine quality existence correctly
    for domain, quality, expected_result in self.td.quality_existence_checks:
      assert self.pm.quality_exists(domain, quality) == expected_result

  def test_pipe_history(self) -> None:
    # Retrieve all of the pipe history as a DataFrame
    df: DataFrame = self.pm.get_pipe_history()
    assert isinstance(df, DataFrame)
    for field in pipe_history.fields:
      assert field.name in df.columns

    # Retrieve the pipe history table of a specific pipe as a DataFrame
    df: DataFrame = self.pm.get_pipe_history('nonexistent_pipe')
    assert isinstance(df, DataFrame)
    for field in pipe_history.fields:
      assert field.name in df.columns
