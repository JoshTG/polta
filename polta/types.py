from polars import DataFrame
from typing import TypeAlias, Union


RawPoltaData: TypeAlias = Union[DataFrame, dict, list[dict]]