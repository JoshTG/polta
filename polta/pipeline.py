from dataclasses import dataclass

from polta.pipe import PoltaPipe


@dataclass
class PoltaPipeline:
  """Simple dataclass for executing chains of pipes for an end product"""
  raw_pipes: list[PoltaPipe]
  conformed_pipes: list[PoltaPipe]
  canonical_pipes: list[PoltaPipe]
  export_pipes: list[PoltaPipe]

  def execute(self, skip_exports: bool = False) -> None:
    """Executes all available pipelines in order of layer
    
    Args:
      skip_exports (bool): indicates whether to skip the export layer (default False)
    """
    for pipe in self.raw_pipes:
      pipe.execute()
    for pipe in self.conformed_pipes:
      pipe.execute()
    for pipe in self.canonical_pipes:
      pipe.execute()
    if not skip_exports:
      for pipe in self.export_pipes:
        pipe.execute()
