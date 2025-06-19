from os import getcwd, path

from polta.metastore import PoltaMetastore


metastore: PoltaMetastore = PoltaMetastore(
  main_path=path.join(getcwd(), 'sample', 'standard', 'test_metastore')
)
