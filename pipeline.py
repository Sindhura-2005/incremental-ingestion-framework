import argparse
import csv
import json
import logging
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
DB_PATH = DATA_DIR / "pipeline.db"
LOG_PATH = REPORTS_DIR / "pipeline.log"

DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

FIELDS = {
    "order_id", "customer_id", "order_amount",
    "status", "updated_at", "source_version", "is_deleted"
}

VALID_STATUSES = {
    "created", "paid", "shipped", "cancelled"
}


def connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orders_current (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
            order_amount TEXT NOT NULL,
            status TEXT NOT NULL,
            source_updated_at TEXT NOT NULL,
            source_version INTEGER NOT NULL,
            is_deleted INTEGER NOT NULL,
            last_run_id TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS order_change_history (
            order_id INTEGER NOT NULL,
            source_version INTEGER NOT NULL,
            source_updated_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            source_file TEXT NOT NULL,
            run_id TEXT NOT NULL,
            PRIMARY KEY (order_id, source_version)
        );

        CREATE TABLE IF NOT EXISTS ingestion_state (
            pipeline_name TEXT PRIMARY KEY,
            committed_watermark TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id TEXT PRIMARY KEY,
            source_file TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            input_count INTEGER NOT NULL DEFAULT 0,
            accepted_count INTEGER NOT NULL DEFAULT 0,
            rejected_count INTEGER NOT NULL DEFAULT 0,
            inserted_count INTEGER NOT NULL DEFAULT 0,
            updated_count INTEGER NOT NULL DEFAULT 0,
            deleted_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS rejected_records (
            reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source_file TEXT NOT NULL,
            row_number INTEGER NOT NULL,
            raw_record TEXT NOT NULL,
            rejection_reason TEXT NOT NULL,
            rejected_at TEXT NOT NULL
        );
        """)

        conn.executemany(
            """INSERT OR IGNORE INTO customers
               (customer_id, customer_name) VALUES (?, ?)""",
            [(i, f"Customer {i}") for i in range(1, 101)],
        )

        conn.execute(
            """INSERT OR IGNORE INTO ingestion_state
               (pipeline_name, committed_watermark)
               VALUES (?, ?)""",
            ("orders", "1970-01-01T00:00:00+00:00"),
        )

    print("Database initialized successfully.")
    print(f"Database location: {DB_PATH}")


def parse_timestamp(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing_updated_at")

    try:
        result = datetime.fromisoformat(
            value.strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("invalid_updated_at") from exc

    if result.tzinfo is None:
        raise ValueError("timestamp_requires_timezone")

    return result.astimezone(timezone.utc)


def validate_record(raw, customer_ids):
    errors = []
    missing = FIELDS - set(raw)

    if missing:
        return None, [
            "missing_columns:" + ",".join(sorted(missing))
        ]

    try:
        order_id = int(raw["order_id"])
        if order_id <= 0:
            raise ValueError()
    except (ValueError, TypeError):
        order_id = None
        errors.append("invalid_order_id")

    try:
        customer_id = int(raw["customer_id"])
        if customer_id not in customer_ids:
            errors.append("unknown_customer_id")
    except (ValueError, TypeError):
        customer_id = None
        errors.append("invalid_customer_id")

    try:
        amount = Decimal(raw["order_amount"])
        if not amount.is_finite() or amount < 0:
            raise ValueError()
        amount_text = format(
            amount.quantize(Decimal("0.01")), "f"
        )
    except (InvalidOperation, ValueError, TypeError):
        amount_text = None
        errors.append("invalid_order_amount")

    status = raw.get("status", "")
    if status not in VALID_STATUSES:
        errors.append("invalid_status")

    try:
        updated_at = parse_timestamp(raw["updated_at"])
    except (ValueError, TypeError, AttributeError):
        updated_at = None
        errors.append("invalid_updated_at")

    try:
        version = int(raw["source_version"])
        if version < 1:
            raise ValueError()
    except (ValueError, TypeError):
        version = None
        errors.append("invalid_source_version")

    deleted_text = raw.get("is_deleted", "").strip().lower()

    if deleted_text in {"true", "1"}:
        is_deleted = 1
    elif deleted_text in {"false", "0"}:
        is_deleted = 0
    else:
        is_deleted = None
        errors.append("invalid_is_deleted")

    if errors:
        return None, errors

    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_amount": amount_text,
        "status": status,
        "updated_at": updated_at,
        "source_version": version,
        "is_deleted": is_deleted,
    }, []

def process_file(
    file_path,
    lookback_minutes=60,
    fail_after_write=False,
):
    init_db()

    file_path = Path(file_path)
    if not file_path.is_absolute():
        file_path = BASE_DIR / file_path

    if not file_path.is_file():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    with connect() as conn:
        conn.execute(
            """INSERT INTO ingestion_runs
               (run_id, source_file, started_at, status)
               VALUES (?, ?, ?, 'running')""",
            (run_id, file_path.name, started_at),
        )

    try:
        with file_path.open(
            "r", newline="", encoding="utf-8-sig"
        ) as file:
            reader = csv.DictReader(file)

            if (
                not reader.fieldnames
                or len(reader.fieldnames) != len(set(reader.fieldnames))
                or set(reader.fieldnames) != FIELDS
            ):
                raise ValueError("Unexpected CSV schema")

            rows = list(reader)

        with connect() as conn:
            customer_ids = {
                row["customer_id"]
                for row in conn.execute(
                    "SELECT customer_id FROM customers"
                )
            }

            state = conn.execute(
                """SELECT committed_watermark FROM ingestion_state
                   WHERE pipeline_name = 'orders'"""
            ).fetchone()

            watermark = parse_timestamp(
                state["committed_watermark"]
            )
            lower_bound = watermark - timedelta(
                minutes=lookback_minutes
            )

            valid_records = []
            rejects = []

            for row_number, raw in enumerate(rows, start=2):
                record, errors = validate_record(raw, customer_ids)

                if errors:
                    rejects.append((row_number, raw, errors))
                else:
                    valid_records.append((row_number, raw, record))

            eligible = [
                item for item in valid_records
                if item[2]["updated_at"] >= lower_bound
            ]

            groups = {}

            for item in eligible:
                record = item[2]
                key = (record["order_id"], record["source_version"])

                payload = {
                    k: v.isoformat() if isinstance(v, datetime) else v
                    for k, v in record.items()
                }
                signature = json.dumps(payload, sort_keys=True)

                groups.setdefault(key, []).append((item, signature))

            deduplicated = []

            for key, group in groups.items():
                signatures = {signature for _, signature in group}

                if len(signatures) > 1:
                    for item, _ in group:
                        rejects.append((
                            item[0], item[1],
                            ["conflicting_same_version"],
                        ))
                else:
                    deduplicated.append(group[0][0])

            versions_by_order = {}

            for item in deduplicated:
                record = item[2]
                versions_by_order.setdefault(
                    record["order_id"], []
                ).append((
                    record["source_version"],
                    record["updated_at"],
                ))

            bad_orders = set()

            for order_id, versions in versions_by_order.items():
                versions.sort(key=lambda item: item[0])

                for previous, current in zip(versions, versions[1:]):
                    if current[1] < previous[1]:
                        bad_orders.add(order_id)

            if bad_orders:
                for item in deduplicated:
                    if item[2]["order_id"] in bad_orders:
                        rejects.append((
                            item[0], item[1],
                            ["timestamp_regressed_with_version"],
                        ))

                deduplicated = [
                    item for item in deduplicated
                    if item[2]["order_id"] not in bad_orders
                ]

            inserted = 0
            updated = 0
            deleted = 0
            history_added = 0
            upper_bound = watermark

            with conn:
                for row_number, raw, errors in rejects:
                    conn.execute(
                        """INSERT INTO rejected_records
                           (run_id, source_file, row_number, raw_record,
                            rejection_reason, rejected_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            run_id, file_path.name, row_number,
                            json.dumps(raw), ";".join(errors),
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    )

                deduplicated.sort(
                    key=lambda item: (
                        item[2]["order_id"],
                        item[2]["source_version"],
                    )
                )

                for _, _, record in deduplicated:
                    timestamp = record["updated_at"].isoformat()
                    upper_bound = max(upper_bound, record["updated_at"])

                    payload = {
                        k: v.isoformat() if isinstance(v, datetime) else v
                        for k, v in record.items()
                    }
                    payload_json = json.dumps(payload, sort_keys=True)

                    existing_event = conn.execute(
                        """SELECT payload_json FROM order_change_history
                           WHERE order_id = ? AND source_version = ?""",
                        (record["order_id"], record["source_version"]),
                    ).fetchone()

                    if existing_event:
                        if existing_event["payload_json"] != payload_json:
                            raise ValueError(
                                "history_version_payload_conflict"
                            )
                        continue

                    old = conn.execute(
                        "SELECT * FROM orders_current WHERE order_id = ?",
                        (record["order_id"],),
                    ).fetchone()

                    if old and record["source_version"] <= old["source_version"]:
                        continue

                    conn.execute(
                        """INSERT INTO order_change_history
                           (order_id, source_version, source_updated_at,
                            payload_json, source_file, run_id)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            record["order_id"], record["source_version"],
                            timestamp, payload_json, file_path.name, run_id,
                        ),
                    )
                    history_added += 1

                    conn.execute(
                        """INSERT INTO orders_current
                           (order_id, customer_id, order_amount, status,
                            source_updated_at, source_version, is_deleted,
                            last_run_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(order_id) DO UPDATE SET
                             customer_id = excluded.customer_id,
                             order_amount = excluded.order_amount,
                             status = excluded.status,
                             source_updated_at = excluded.source_updated_at,
                             source_version = excluded.source_version,
                             is_deleted = excluded.is_deleted,
                             last_run_id = excluded.last_run_id
                           WHERE excluded.source_version >
                                 orders_current.source_version""",
                        (
                            record["order_id"], record["customer_id"],
                            record["order_amount"], record["status"],
                            timestamp, record["source_version"],
                            record["is_deleted"], run_id,
                        ),
                    )

                    if old is None:
                        inserted += 1
                    elif record["is_deleted"] == 1:
                        deleted += 1
                    else:
                        updated += 1

                if fail_after_write:
                    raise RuntimeError(
                        "simulated_failure_before_watermark"
                    )

                conn.execute(
                    """UPDATE ingestion_state SET committed_watermark = ?
                       WHERE pipeline_name = 'orders'""",
                    (upper_bound.isoformat(),),
                )

                conn.execute(
                    """UPDATE ingestion_runs SET ended_at = ?,
                       status = 'success', input_count = ?,
                       accepted_count = ?, rejected_count = ?,
                       inserted_count = ?, updated_count = ?,
                       deleted_count = ?
                       WHERE run_id = ?""",
                    (
                        datetime.now(timezone.utc).isoformat(),
                        len(rows), history_added, len(rejects),
                        inserted, updated, deleted, run_id,
                    ),
                )

        logging.info("Run %s succeeded", run_id)

        print(json.dumps({
            "run_id": run_id,
            "status": "success",
            "input_count": len(rows),
            "accepted_count": history_added,
            "rejected_count": len(rejects),
            "inserted_count": inserted,
            "updated_count": updated,
            "deleted_count": deleted,
            "watermark": upper_bound.isoformat(),
        }, indent=2))

        return run_id

    except Exception as exc:
        with connect() as conn:
            conn.execute(
                """UPDATE ingestion_runs SET ended_at = ?,
                   status = 'failed', error_message = ?
                   WHERE run_id = ?""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    str(exc), run_id,
                ),
            )

        logging.exception("Run %s failed", run_id)
        print(f"Pipeline failed: {exc}")
        raise
    def main():
        parser = argparse.ArgumentParser(
        description="Incremental Order Ingestion Pipeline"
        )

        parser.add_argument("--init", action="store_true")
        parser.add_argument("--input")
        parser.add_argument("--lookback-minutes", type=int, default=60)
        parser.add_argument("--simulate-failure", action="store_true")

        args = parser.parse_args()

        if args.lookback_minutes < 0:
            parser.error("--lookback-minutes cannot be negative")

        if args.init:
            init_db()
        elif args.input:
            process_file(
                args.input,
                lookback_minutes=args.lookback_minutes,
                fail_after_write=args.simulate_failure,
            )
        else:
            parser.error("Use --init or --input PATH")


if __name__ == "__main__":
    main()