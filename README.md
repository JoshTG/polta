# Polta
## Introduction
The `polta` module combines `polars` data transformation with `deltalake` table storage in a unified wrapper for common Data Engineering tasks.

## Installation
Currently, `polta` can be accessed only by cloning this repository, building the package via `poetry`, and importing the wheel into your project.

Future state will include an official `PyPi` package.

## Usage

### PoltaTable

Below is an example instantiation of a `PoltaTable`.

```python
from deltalake import Field, Schema

from polta.table import PoltaTable


sample_table: PoltaTable = PoltaTable(
  catalog_name='tests',
  schema_name='testing_data',
  table_name='test_table',
  raw_schema=Schema([
    Field('id', 'integer'),
    Field('name', 'string'),
    Field('active_ind', 'boolean')
  ]),
  primary_keys=['id', 'name']
)
```

For the `raw_schema` field, you can provide either a `deltalake` schema, like what is provided, or a `dict[str, DataType]` object, where `DataType` is a `polars` data type object.

From here, you will be able to do the following commands and more:
1. Retrieve the table as a DataFrame with `sample_table.get()`.
2. Retrieve the table as a DeltaTable with `sample_table.get_as_delta_table()`.
3. Append data to the table with `sample_table.append(data)`.
4. Upsert data to the table with `sample_table.upsert(data)`.
5. Overwrite the table with new data with `sample_table.overwrite(data)`.
6. Truncate the table with `sample_table.truncate()`.
7. Drop the table with `sample_table.drop()`.

For the append/upsert/overwrite, you can input either an individual record as a `dict` object, a `list[dict]` object, or a DataFrame.

Look at the inline docs for more information on usage, as well as the additional methods in the class itself.


## License

This project is covered under the `MIT License`.


## Contributing to this Project

As an open-source project, all meaningful contributions are welcome by following these steps:
1. Fork the repository in your own GitHub account.
2. Implement, test, and commit your changes.
3. Uptick the `poetry` version appropriately in the `pyproject.toml` file and refresh the `poetry.lock` file with a `poetry install` command.
4. Submit a merge request into the official repository and assign me as a reviewer.

## Project Owner
This project is built and maintained by Joshua Gilliland (@JoshTG).

© 2025