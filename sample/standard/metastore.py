from os import getcwd, path

from polta.metastore import Metastore


metastore: Metastore = Metastore(
  main_path=path.join(getcwd(), 'sample', 'standard', 'test_metastore')
)
