from os import path
from shutil import rmtree
from unittest import TestCase

from polta.enums import TableQuality
from polta.exceptions import DomainDoesNotExist
from polta.metastore import Metastore
from sample.metastore import metastore, metastore_init
from sample.standard.pipelines.user import pip_can_user
from tests.unit.testing_data.metastore import TestingData


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
  