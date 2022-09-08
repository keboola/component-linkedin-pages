from dataclasses import dataclass
from typing import Iterable
import os
import csv

from keboola.component.base import ComponentBase
from keboola.component.dao import TableMetadata


@dataclass(slots=True, frozen=True)
class Table:
    name: str
    columns: list[str]
    primary_key: list[str]
    records: Iterable[dict]
    metadata: TableMetadata | None = None
    delete_where_spec: dict | None = None


def save_as_csv_with_manifest(table: Table, component: ComponentBase, incremental: bool):
    table_def = component.create_out_table_definition(name=f"{table.name}.csv",
                                                      is_sliced=False,
                                                      primary_key=table.primary_key,
                                                      columns=table.columns,
                                                      incremental=incremental,
                                                      table_metadata=table.metadata,
                                                      delete_where=table.delete_where_spec)
    os.makedirs(component.tables_out_path, exist_ok=True)
    with open(os.path.join(component.tables_out_path, table_def.name), "w") as f:
        csv_writer = csv.DictWriter(f, dialect='kbc', fieldnames=table_def.columns)
        csv_writer.writerows(table.records)
    component.write_manifest(table_def)
