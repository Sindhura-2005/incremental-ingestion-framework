import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "pipeline.db"
OUTPUT_PATH = ROOT / "reports" / "validation_report.json"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    current_count = conn.execute(
        "SELECT COUNT(*) FROM orders_current"
    ).fetchone()[0]

    active_count = conn.execute(
        "SELECT COUNT(*) FROM orders_current WHERE is_deleted = 0"
    ).fetchone()[0]

    deleted_count = conn.execute(
        "SELECT COUNT(*) FROM orders_current WHERE is_deleted = 1"
    ).fetchone()[0]

    duplicate_keys = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id
            FROM orders_current
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    rejected_count = conn.execute(
        "SELECT COUNT(*) FROM rejected_records"
    ).fetchone()[0]

    successful_runs = conn.execute(
        "SELECT COUNT(*) FROM ingestion_runs WHERE status = 'success'"
    ).fetchone()[0]

    failed_runs = conn.execute(
        "SELECT COUNT(*) FROM ingestion_runs WHERE status = 'failed'"
    ).fetchone()[0]

    history_count = conn.execute(
        "SELECT COUNT(*) FROM order_change_history"
    ).fetchone()[0]

    watermark = conn.execute(
        """SELECT committed_watermark
           FROM ingestion_state
           WHERE pipeline_name = 'orders'"""
    ).fetchone()[0]

    conn.close()

    report = {
        "current_orders": current_count,
        "active_orders": active_count,
        "deleted_orders": deleted_count,
        "duplicate_order_keys": duplicate_keys,
        "rejected_records": rejected_count,
        "successful_runs": successful_runs,
        "failed_runs": failed_runs,
        "history_events": history_count,
        "committed_watermark": watermark,
        "checks": {
            "no_duplicate_order_keys": duplicate_keys == 0,
            "active_plus_deleted_equals_total": (
                active_count + deleted_count == current_count
            ),
            "database_has_orders": current_count > 0,
        },
    }

    report["overall_pass"] = all(report["checks"].values())

    OUTPUT_PATH.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print(json.dumps(report, indent=2))
    print(f"\nReport saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()