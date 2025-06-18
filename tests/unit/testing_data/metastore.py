from polta.enums import TableQuality


class TestingData:
  domains: list[str] = ['in_memory', 'standard']
  qualities: list[TableQuality] = [
    TableQuality.CANONICAL,
    TableQuality.CONFORMED,
    TableQuality.RAW
  ]
  domain_existence_checks: list[tuple[str, bool]] = [
    ('standard', True),
    ('in_memory', True),
    ('plankton', False)
  ]
  quality_existence_checks: list[tuple[str, TableQuality, bool]] = [
    ('standard', TableQuality.CANONICAL, True),
    ('standard', TableQuality.CONFORMED, True),
    ('standard', TableQuality.RAW, True),
  ]
