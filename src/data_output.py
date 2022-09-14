from dataclasses import dataclass
from typing import Iterable
import os

from keboola.component.base import ComponentBase
from keboola.component.dao import TableMetadata

from csv_tools import CachedOrthogonalDictWriter


@dataclass(slots=True, frozen=True)
class Table:
    name: str
    columns: list[str]
    primary_key: list[str]
    records: Iterable[dict]
    metadata: TableMetadata | None = None
    delete_where_spec: dict | None = None

    def save_as_csv_with_manifest(self, component: ComponentBase, incremental: bool, include_csv_header: bool = False):
        table_def = component.create_out_table_definition(name=f"{self.name}.csv",
                                                          is_sliced=False,
                                                          primary_key=self.primary_key,
                                                          columns=self.columns,
                                                          incremental=incremental,
                                                          table_metadata=self.metadata,
                                                          delete_where=self.delete_where_spec)
        os.makedirs(component.tables_out_path, exist_ok=True)
        with CachedOrthogonalDictWriter(os.path.join(component.tables_out_path, table_def.name),
                                        dialect='kbc',
                                        fieldnames=table_def.columns.copy()) as csv_writer:
            if include_csv_header:
                csv_writer.writeheader()
            csv_writer.writerows(self.records)
        table_def.columns = csv_writer.fieldnames
        component.write_manifest(table_def)
