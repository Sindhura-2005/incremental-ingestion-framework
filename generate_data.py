import csv
import random
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

FIELDS = [
    "order_id",
    "customer_id",
    "order_amount",
    "status",
    "updated_at",
    "source_version",
    "is_deleted",
]

STATUSES = ["created", "paid", "shipped", "cancelled"]


def generate_data(count=100, seed=42):
    random.seed(seed)
    DATA_DIR.mkdir(exist_ok=True)

    output_file = DATA_DIR / "orders.csv"
    now = datetime.now(timezone.utc).replace(microsecond=0)

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()

        for order_id in range(1000001, count + 1000001):
            updated_at = now - timedelta(minutes=random.randint(0, 500))
            writer.writerow({
                "order_id": order_id,
                "customer_id": random.randint(1, 100),
                "order_amount": f"{random.uniform(10, 1000):.2f}",
                "status": random.choice(STATUSES),
                "updated_at": updated_at.isoformat(),
                "source_version": 1,
                "is_deleted": "false",
            })

    print(f"Generated {count} orders.")
    print(f"CSV file: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample order CSV data"
    )
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.count < 0:
        parser.error("--count cannot be negative")

    generate_data(args.count, args.seed)


if __name__ == "__main__":
    main()