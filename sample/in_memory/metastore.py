from os import getcwd, path

from polta.metastore import PoltaMetastore


metastore: PoltaMetastore = PoltaMetastore(
  main_path=path.join(getcwd(), 'sample', 'in_memory', 'test_metastore')
)