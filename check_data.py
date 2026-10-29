import sqlite3

conn = sqlite3.connect("data/pipeline.db")

print("\n--- Current order counts ---")
print(conn.execute(
    "SELECT COUNT(*) FROM orders_current"
).fetchone()[0])

print("\n--- Duplicate order IDs ---")
print(conn.execute("""
    SELECT order_id, COUNT(*)
    FROM orders_current
    GROUP BY order_id
    HAVING COUNT(*) > 1
""").fetchall())

print("\n--- Ingestion run history ---")
for row in conn.execute("""
    SELECT status, input_count, accepted_count,
           rejected_count, inserted_count
    FROM ingestion_runs
    ORDER BY started_at
"""):
    print(row)

print("\n--- Rejected records ---")
print(conn.execute(
    "SELECT COUNT(*) FROM rejected_records"
).fetchone()[0])

conn.close()