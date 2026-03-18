import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.experiments.db_adapter import make_adapter

dbs = [
    ("PG", "postgres", "postgresql://localhost:5432/postgres"),
    ("PG 12.3", "postgres", "postgresql://localhost:54323/postgres"),
    ("PG 12.4", "postgres", "postgresql://localhost:54324/postgres"),
    ("MySQL 33080", "mysql", "mysql://root:root@localhost:33080/test"),
    ("MySQL 3307", "mysql", "mysql://root:root@localhost:3307/test"),
]
for name, db, dsn in dbs:
    try:
        a = make_adapter(db, dsn)
        a.close()
        print(f"{name}: OK")
    except Exception as e:
        print(f"{name}: FAIL ({e})")
print("SQLite: OK")
