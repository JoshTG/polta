from enum import Enum


class TableQuality(Enum):
  RAW = 'raw'
  CONFORMED = 'conformed'
  CANONICAL = 'canonical'

class LoadLogic(Enum):
  APPEND = 'append'
  OVERWRITE = 'overwrite'
  UPSERT = 'upsert'
