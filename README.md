# Polta
_Data engineering tool combining Polars transformations with Delta tables/lakes._

# Core Concepts
The `polta` module revolves around the following core objects that, in conjunction with each other, allow you to create small-to-medium-scale pipelines.

### PoltaMetastore

Every `polta` integration should have a dedicated metastore for preserving data and logs. This is automatically created and managed by `polta` before executing any transformations or reads.

There are two main aspects of a `PoltaMetastore`:

1. *Tables*: Contains every table across all layers.
2. *Volumes*: Contains file storage systems needed for transformations.

This structure is inspired by `deltalake` and follows similar metastore paradigms.

It loosely follows the modern [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) language for organizing the data layers, with these naming conventions for each layer:

1. *Raw*: Source data, usually a payload string.
2. *Conformed*: Structured data.
3. *Canonical*: Business-level data.

If the data can be conformed easily, it may get loaded from the ingestion zone into _conformed_. Otherwise, it should get loaded into _raw_.

### PoltaTable

The `PoltaTable` is the primary way to read and write data.

It stores data using `deltalake`, and it transforms data using `polars`. Because it integrates two modules together, it has many fields and methods for communicating seamlessly to and fro. For example, every `PoltaTable` has readily available a `schema_polars` and `schema_deltalake` object that both represent your table schema.

Each raw `PoltaTable` has a dedicated ingestion zone located in the `PoltaMetastore` to store sources files ready to be loaded into the raw layer.

### PoltaIngester

The `PoltaIngester` is the primary way to load source files into the raw layer.

It currently supports ingesting these formats:

1. JSON
2. String payload

An instance can get passed into a `PoltaPipe` to ingest data into a `PoltaTable`.

### PoltaPipe

The `PoltaPipe` is the primary way to transform data from one location into a `PoltaTable`.

### PoltaPipeline

The `PoltaPipeline` is the primary way to link `PoltaPipe` objects together to create a unified data pipeline.

# Installation


## Installing to a Project
This project exists in `PyPI` and can be installed this way:

```sh
pip install polta
```

## Initializing the Repository

To use the code from the repository itself, either for testing or contributing, follow these steps:

1. Clone the repository to your local machine.
2. Create a virtual environment, preferably using `venv`, that runs `Python 3.13`. 
3. Ensure you have `poetry` installed (installation instructions [here](https://python-poetry.org/docs/#installation)).
4. Make `poetry` use the virtual environment using `poetry env use .venv/path/to/python`.
5. Download dependencies by executing `poetry install`.
6. Building a wheel file by executing `poetry build`.

# Testing

This project uses `pytest` for its tests, all of which exist in the `tests` directory. Below are recommended testing options.

### VS Code (Preferred)

There is a `Testing` tab in the left-most menu by default that allows you to run `pytest` tests in bulk or individually.

### Poetry
To execute tests using `poetry`, run this command in your terminal at the top-level directory:
```sh
poetry run pytest tests/ -vv -s
```

# Usage

Below are sample code snippets to show basic usage. For a full sample pipeline, consult the `sample` directory in the repository for an example pipeline. These tables, pipes, and pipeline get used in the integration test which is located in the `tests/integration/test_pipeline.py` pytest file.

Below is a diagram of the basic pipeline architecture with these features:

- The columns represent logical layers where data is stored.
- The rows represent the two kinds of data within the metastore.
- The pipes represent `PoltaPipe` objects.
- The rectangles represent `PoltaTable` objects.
- The rectangles with wavy bottom sides represent directories in the ingestion zone.

![polta-diagram](assets/png/polta-data-flow-diagram.png)

## Sample Metastore

The creation of a new metastore is simple. Below is a sample metastore that can be passed into the initialization of any `PoltaTable` to ensure the table writes to the metastore.

```python
from polta.metastore import PoltaMetastore


metastore: PoltaMetastore = PoltaMetastore('path/to/desired/store')
```

## Sample Simple PoltaPipe

This sample code illustrates a simple raw ingestion pipe.

A pipe file typically contains a `PoltaTable` and a `PoltaPipe`, and a raw table might have an additional `PoltaIngester`.

```python
from deltalake import Field, Schema

from polta.enums import (
  DirectoryType,
  LoadLogic,
  RawFileType,
  TableQuality
)
from polta.ingester import PoltaIngester
from polta.pipe import PoltaPipe
from polta.table import PoltaTable

from .metastore import metastore


table: PoltaTable = PoltaTable(
  domain='sample',
  quality=TableQuality.RAW,
  name='table',
  raw_schema=Schema([
    Field('payload', 'string')
  ]),
  metastore=metastore
)

ingester: PoltaIngester = PoltaIngester(
  table=table,
  directory_type=DirectoryType.SHALLOW,
  raw_file_type=RawFileType.JSON
)

pipe: PoltaPipe = PoltaPipe(
  table=table,
  load_logic=LoadLogic.APPEND,
  ingester=ingester
)
```

By making `table.raw_schema` a simple payload, that signals to the ingester that the transformation is a simple file read.

This code is all that is needed to execute a load of all data from the ingestion zone to a raw table. To do so, execute `pipe.execute()`.

If you want to read the data, execute `table.get()`.

## Sample Complex PoltaPipe

For instances where transformation logic is required, you must create a child `PoltaPipe` class that overrides the load and transform methods with your own custom code, as sampled below.

```python
from polars import col, DataFrame
from polars.datatypes import DataType, List, Struct

from polta.enums import LoadLogic
from polta.maps import PoltaMaps
from polta.pipe import PoltaPipe
from polta.table import PoltaTable
from polta.udfs import string_to_struct
from sample.table import \
  table as pt_raw_table


class SampleComplexPipe(PoltaPipe):
  """Pipe to load sample data into a conformed model"""
  def __init__(self, table: PoltaTable) -> None:
    super().__init__(table, LoadLogic.APPEND)
    self.raw_polars_schema: dict[str, DataType] = PoltaMaps \
      .deltalake_schema_to_polars_schema(self.table.raw_schema)
  
  def load_dfs(self) -> dict[str, DataFrame]:
    """Basic load logic:
      1. Get raw table data as a DataFrame
      2. Anti join against conformed layer to get net-new records
    
    Returns:
      dfs (dict[str, DataFrame]): The resulting data as 'table'
    """
    conformed_ids: DataFrame = self.table.get(select=['_raw_id'], unique=True)
    df: DataFrame = (pt_raw_table
      .get()
      .join(conformed_ids, '_raw_id', 'anti')
    )
    return {'table': df}

  def transform(self) -> DataFrame:
    """Basic transformation logic:
      1. Retrieve the raw table DataFrame
      2. Convert 'payload' into a struct
      3. Explode the struct
      4. Convert the struct key-value pairs into column-cell values

    Returns:
      df (DataFrame): the resulting DataFrame
    """
    df: DataFrame = self.dfs['table']

    return (df
      .with_columns([
        col('payload')
          .map_elements(string_to_struct, return_dtype=List(Struct(self.raw_polars_schema)))
      ])
      .explode('payload')
      .with_columns([
        col('payload').struct.field(f).alias(f)
        for f in [n.name for n in self.table.raw_schema.fields]
      ])
      .drop('payload')
    )
```

This child class receives the raw data from the previous example, explodes the data, and extracts the proper fields into a proper conformed DataFrame.

The `PoltaPipe` instance is sampled below.

```python
from deltalake import Field, Schema

from polta.enums import TableQuality
from polta.table import PoltaTable
from .pipes.sample import SampleComplexPipe
from .metastore import metastore


table: PoltaTable = PoltaTable(
  domain='test',
  quality=TableQuality.CONFORMED,
  name='table',
  raw_schema=Schema([
    Field('id', 'string'),
    Field('active_ind', 'boolean')
  ]),
  metastore=metastore
)

pipe: SampleComplexPipe = SampleComplexPipe(table)
```

From there, the pipe can be executed by running `pipe.execute()`, and any new raw files will get transformed and loaded into the conformed layer.

## Sample PoltaPipeline

To connect the above pipes together, you can create a `PoltaPipeline`, as sampled below.

```python
from polta.pipeline import PoltaPipeline

from sample.raw.table import \
  pipe as pp_raw_sample
from sample.conformed.table import \
  pipe as pp_con_sample


pipeline: PoltaPipeline = PoltaPipeline(
  raw_pipes=[pp_raw_sample],
  conformed_pipes=[pp_con_sample]
)
```

You can then execute your pipeline by running `pipeline.execute()`.

# License

This project exists under the `MIT License`. Consult the `LICENSE` file in this repository for more information on what that means.

# Contributing

Because this project is open-source, contributions are most welcome.

To contribute, follow these steps:

1. Clone the repository into your local machine.
2. Create a descriptive feature branch.
3. Make the desired changes.
4. Fully test the desired changes using the `unit` and `integration` test directories in the `tests` directory.
5. Uptick the `poetry` project version appropriately using standard semantic versioning.
6. Create a merge request into the `main` branch of the official `polta` project.
7. Once the merge request is approved and merged, an administrator will schedule a release cycle and deploy the changes using a new release tag.

# Contact

You may contact the main contributor, @JoshTG, by sending an email to this address: jgillilanddata@gmail.com
