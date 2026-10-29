Incremental Ingestion Framework
Project Overview

This project implements a local incremental data ingestion pipeline using Python, CSV, and SQLite.

It validates incoming order records, handles duplicate events, maintains a current-state table and change history, supports deletion tombstones, and records ingestion run status.

Architecture
text
Source CSV
    |
    v
Schema and data validation
    |
    +---- Invalid records ----> rejected_records
    |
    v
Duplicate and version checks
    |
    v
SQLite transaction
    |
    +----> orders_current
    |
    +----> order_change_history
    |
    +----> ingestion_state
    |
    v
Validation report

Technology
Python 3
SQLite
CSV
unittest

No external database or paid cloud service is required.

Setup

Open a terminal in the project root.

Initialize the database:

bash
python pipeline.py --init


Generate sample data:

bash
python generate_data.py


Run ingestion:

bash
python pipeline.py --input data/orders.csv --lookback-minutes 600


Run automated tests:

bash
python -m unittest discover -s tests -v


Generate the validation report:

bash
python reports/validation_report.py


Generate a larger dataset:

bash
python generate_data.py --count 100000

Database Tables
customers: reference customer data.
orders_current: latest accepted state of each order, including deletion tombstones.
order_change_history: accepted order versions.
ingestion_state: committed ingestion watermark.
ingestion_runs: execution status and operational counts.
rejected_records: invalid or conflicting records with source details.
Data Quality Rules
Order IDs must be positive integers.
Customer IDs must reference known customers.
Amounts must be non-negative numeric values.
Status must be created, paid, shipped, or cancelled.
Update timestamps must include a timezone.
Source versions must be positive integers.
Deletion flags must be valid boolean values.
Idempotency and Recovery

The order ID is the current-state key. Source versions prevent older versions from overwriting newer versions. The history table prevents a source version from being stored more than once.

Current-state changes and watermark updates are committed in a database transaction. A failed transaction can be retried because uncommitted changes are rolled back.

Validation Evidence

The project includes automated tests and a generated JSON validation report. Run both before submitting.

Limitations and Future Improvements
The reference customer dataset is synthetic.
The pipeline uses a timestamp lookback window; records older than the window may require an explicit backfill.
Production deployments should use source-provided change sequence numbers or CDC where available.
Additional integration tests should cover late-arriving events, tombstones, malformed CSV files, and simulated transaction failures.
Large-scale deployments could use partitioned files and a distributed processing engine.