"""Capture current SQLAlchemy metadata baseline before migration"""
import json
from app.core.database import Base

# Import models to register them
from app.db import models  # noqa: F401

metadata_snapshot = {}

for table_name, table in Base.metadata.tables.items():
    table_info = {
        "name": table_name,
        "columns": {},
        "primary_keys": [],
        "foreign_keys": [],
        "unique_constraints": [],
        "indexes": [],
    }

    # Capture column details
    for column in table.columns:
        col_info = {
            "name": column.name,
            "type": str(column.type),
            "nullable": column.nullable,
            "primary_key": column.primary_key,
            "unique": column.unique,
            "default": str(column.default) if column.default is not None else None,
            "server_default": str(column.server_default) if column.server_default is not None else None,
            "onupdate": str(column.onupdate) if column.onupdate is not None else None,
        }
        table_info["columns"][column.name] = col_info

    # Capture primary keys
    table_info["primary_keys"] = [col.name for col in table.primary_key.columns]

    # Capture foreign keys
    for fk in table.foreign_keys:
        table_info["foreign_keys"].append({
            "parent_column": fk.parent.name,
            "referenced_table": fk.column.table.name,
            "referenced_column": fk.column.name,
        })

    # Capture unique constraints
    for constraint in table.constraints:
        if constraint.__class__.__name__ == 'UniqueConstraint':
            table_info["unique_constraints"].append({
                "name": constraint.name,
                "columns": [col.name for col in constraint.columns]
            })

    # Capture indexes
    for index in table.indexes:
        table_info["indexes"].append({
            "name": index.name,
            "columns": [col.name for col in index.columns],
            "unique": index.unique
        })

    metadata_snapshot[table_name] = table_info

print(json.dumps(metadata_snapshot, indent=2, sort_keys=True))
