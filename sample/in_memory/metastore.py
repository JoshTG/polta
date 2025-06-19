from os import getcwd, path

from polta.metastore import Metastore


metastore: Metastore = Metastore(
  main_path=path.join(getcwd(), 'sample', 'in_memory', 'test_metastore')
)