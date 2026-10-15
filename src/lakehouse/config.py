import os
from pathlib import Path

ROOT = Path(os.environ.get("LAKEHOUSE_HOME", Path.cwd() / "lakehouse_data")).resolve()
LANDING = ROOT / "landing"          # stands in for an S3 landing bucket
WAREHOUSE = ROOT / "warehouse"      # Iceberg data + metadata files
CATALOG_DB = ROOT / "catalog.db"    # SQL catalog (Glue / REST catalog in production)
