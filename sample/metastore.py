from os import getcwd, path

from polta.metastore import PoltaMetastore


# To hold data for test cases
metastore: PoltaMetastore = PoltaMetastore(
  main_path=path.join(getcwd(), 'sample', 'test_metastore')
)

# For destroying and rebuilding as needed
metastore_init: PoltaMetastore = PoltaMetastore(
  main_path=path.join(getcwd(), 'sample', 'test_metastore_init')
)
