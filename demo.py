"""
WEAVE-DB: Full Pipeline Demo
=============================
Demonstrates the complete inverse-command pipeline against a LIVE PostgreSQL
instance.

Prerequisites
-------------
1.  PostgreSQL running locally (or via Docker):
        docker run -d --name pgdev -p 5432:5432 \\
            -e POSTGRES_PASSWORD=password postgres:15

2.  psycopg2 installed:
        pip install psycopg2-binary

3.  Update DSN below if needed.

What this demo does
-------------------
Step 1  : Connect and initialise WEAVE-DB
Step 2  : CREATE TABLE   → inverse: DROP TABLE
Step 3  : INSERT rows    → inverse: DELETE (per PK)
Step 4  : UPDATE rows    → inverse: UPDATE (restoring before-image)
Step 5  : DELETE a row   → inverse: INSERT (re-inserting original)
Step 6  : ALTER TABLE ADD COLUMN → inverse: DROP COLUMN
Step 7  : RENAME TABLE   → inverse: RENAME back
Step 8  : TRUNCATE       → inverse: INSERT all rows
Step 9  : CREATE INDEX   → inverse: DROP INDEX
Step 10 : Print history
Step 11 : Rollback to version 3 (after second INSERT)
Step 12 : Verify state with SELECT
"""

import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(name)s  %(message)s"
)

DSN = "postgresql://postgres:hive123@localhost:5432/weavedb_test"

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 not installed.  Run: pip install psycopg2-binary")
    sys.exit(1)

from workbench import Workbench


def banner(msg):
    print(f"\n{'─'*60}")
    print(f"  {msg}")
    print(f"{'─'*60}")


def main():
    banner("Connecting to PostgreSQL")
    try:
        conn = psycopg2.connect(DSN, cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as exc:
        print(f"Connection failed: {exc}")
        print(f"Tried DSN: {DSN}")
        print("Make sure PostgreSQL is running and update DSN in this script.")
        sys.exit(1)

    print("Connected ✓")

    # Drop demo table if it exists from a previous run
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS weavedb_demo CASCADE;")
    cur.execute("DROP TABLE IF EXISTS weavedb_demo_renamed CASCADE;")
    conn.commit()

    wb = Workbench(conn, author="demo_user", snapshot_every=5)

    # ── Step 2: CREATE TABLE ──────────────────────────────────────────
    banner("Step 2 – CREATE TABLE")
    r = wb.execute("""
        CREATE TABLE weavedb_demo (
            id    SERIAL PRIMARY KEY,
            name  TEXT        NOT NULL,
            score NUMERIC(5,2),
            active BOOLEAN    DEFAULT TRUE
        )
    """)
    print(f"version_id={r.version_id}  category={r.category}  reversible={r.is_reversible}")

    # ── Step 3: INSERT rows ───────────────────────────────────────────
    banner("Step 3 – INSERT rows")
    for name, score in [("Alice", 95.5), ("Bob", 78.0), ("Carol", 88.3)]:
        r = wb.execute(
            "INSERT INTO weavedb_demo (name, score) VALUES (%s, %s) RETURNING *",
            (name, score)
        )
        print(f"  Inserted {name}: version_id={r.version_id}")

    # ── Step 4: UPDATE ────────────────────────────────────────────────
    banner("Step 4 – UPDATE")
    r = wb.execute("UPDATE weavedb_demo SET score = 100.0 WHERE name = 'Alice'")
    print(f"version_id={r.version_id}  rows_affected={r.rows_affected}")

    # ── Step 5: DELETE ────────────────────────────────────────────────
    banner("Step 5 – DELETE")
    r = wb.execute("DELETE FROM weavedb_demo WHERE name = 'Bob'")
    print(f"version_id={r.version_id}  rows_affected={r.rows_affected}")

    # ── Step 6: ALTER TABLE ADD COLUMN ───────────────────────────────
    banner("Step 6 – ALTER TABLE ADD COLUMN")
    r = wb.execute("ALTER TABLE weavedb_demo ADD COLUMN notes TEXT")
    print(f"version_id={r.version_id}  reversible={r.is_reversible}")

    # ── Step 7: RENAME TABLE ──────────────────────────────────────────
    banner("Step 7 – RENAME TABLE")
    r = wb.execute("ALTER TABLE weavedb_demo RENAME TO weavedb_demo_renamed")
    print(f"version_id={r.version_id}  reversible={r.is_reversible}")

    # Rename back so rest of demo still works
    wb.execute("ALTER TABLE weavedb_demo_renamed RENAME TO weavedb_demo")

    # ── Step 8: TRUNCATE ──────────────────────────────────────────────
    banner("Step 8 – TRUNCATE (will capture all rows first)")
    r = wb.execute("TRUNCATE TABLE weavedb_demo_renamed")
    print(f"version_id={r.version_id}  reversible={r.is_reversible}")

    # Re-insert after truncate for further steps
    wb.execute("INSERT INTO weavedb_demo (name, score) VALUES ('Dave', 70.0) RETURNING *")

    # ── Step 9: CREATE INDEX ──────────────────────────────────────────
    banner("Step 9 – CREATE INDEX")
    r = wb.execute("CREATE INDEX idx_demo_name ON weavedb_demo (name)")
    print(f"version_id={r.version_id}  reversible={r.is_reversible}")

    # ── Step 10: Print history ────────────────────────────────────────
    banner("Step 10 – Commit History")
    wb.print_history()

    # ── Step 11: Inspect an inverse command ──────────────────────────
    banner("Step 11 – Inspect inverse for version 5 (DELETE)")
    inv_dict = wb.inspect_inverse(5)
    if inv_dict:
        import json
        print(json.dumps(inv_dict, indent=2, default=str))

    # ── Step 12: Dry-run rollback plan ───────────────────────────────
    banner("Step 12 – Dry-run rollback plan to version 3")
    plan = wb.plan_rollback(target_version=3, table_name="weavedb_demo")
    print("Plan steps:")
    for step in plan.steps_applied:
        print(" ", step)

    # ── Step 13: Actual rollback ──────────────────────────────────────
    banner("Step 13 – Rollback to version 3 (after second INSERT)")
    result = wb.rollback_to(target_version=3, table_name="weavedb_demo")
    print(result)
    if result.success:
        print("Steps applied:")
        for step in result.steps_applied:
            print(" ", step[:100])

    # ── Step 14: Verify state ─────────────────────────────────────────
    banner("Step 14 – Current table state after rollback")
    sel = wb.execute("SELECT * FROM weavedb_demo ORDER BY id")
    if sel.success and sel.rows:
        print(f"Columns: {sel.columns}")
        for row in sel.rows:
            print(" ", dict(row) if hasattr(row, 'items') else row)
    else:
        print("(no rows or table doesn't exist – check rollback scope)")

    banner("Demo complete ✓")
    conn.close()


if __name__ == "__main__":
    main()
