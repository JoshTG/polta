from os import getcwd, path

from polta.metastore import Metastore


# To hold data for test cases
metastore: Metastore = Metastore(
  main_path=path.join(getcwd(), 'sample', 'test_metastore')
)

# For destroying and rebuilding as needed
metastore_init: Metastore = Metastore(
  main_path=path.join(getcwd(), 'sample', 'test_metastore_init')
)
