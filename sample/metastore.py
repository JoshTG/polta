from os import getcwd, path

from polta.metastore import PoltaMetastore


sample_metastore: PoltaMetastore = PoltaMetastore(
  main_path=path.join(getcwd(), 'sample', 'test_metastore')
)
