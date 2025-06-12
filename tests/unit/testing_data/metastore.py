from polta.enums import TableQuality


class TestingData:
  domains: list[str] = ['test']
  qualities: list[TableQuality] = [
    TableQuality.CANONICAL,
    TableQuality.CONFORMED,
    TableQuality.RAW
  ]
  domain_existence_checks: list[tuple[str, bool]] = [
    ('test', True),
    ('plankton', False)
  ]
  quality_existence_checks: list[tuple[str, TableQuality, bool]] = [
    ('test', TableQuality.CANONICAL, True),
    ('test', TableQuality.CONFORMED, True),
    ('test', TableQuality.EXPORT, False),
    ('test', TableQuality.RAW, True),
  ]
