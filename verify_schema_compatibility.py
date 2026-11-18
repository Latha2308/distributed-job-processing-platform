"""Verify schema compatibility before and after migration"""
import json
from app.core.database import Base
from app.db import models  # noqa: F401

def capture_metadata():
    """Capture complete table metadata"""
    snapshot = {}

    for table_name, table in sorted(Base.metadata.tables.items()):
        info = {
            "name": table_name,
            "columns": {}
        }

        for col in table.columns:
            info["columns"][col.name] = {
                "type": str(col.type),
                "nullable": col.nullable,
                "primary_key": col.primary_key,
                "unique": col.unique,
                "default": str(col.default) if col.default else None,
                "onupdate": str(col.onupdate) if col.onupdate else None,
            }

        info["indexes"] = [
            {
                "name": idx.name,
                "columns": [c.name for c in idx.columns],
                "unique": idx.unique
            }
            for idx in sorted(table.indexes, key=lambda x: x.name or "")
        ]

        snapshot[table_name] = info

    return snapshot

if __name__ == "__main__":
    metadata = capture_metadata()
    print(json.dumps(metadata, indent=2, sort_keys=True))
