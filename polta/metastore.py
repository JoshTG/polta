from dataclasses import dataclass, field
from os import makedirs, path


@dataclass
class PoltaMetastore:
  """Dataclass for managing Polta metastores"""
  main_path: str

  def __post_init__(self) -> None:
    self.initialize_if_not_exists()

  def initialize_if_not_exists(self) -> None:
    """Initializes the metastore if it does not exist"""
    if path.exists(self.main_path):
      return

    makedirs(self.main_path, exist_ok=True)
