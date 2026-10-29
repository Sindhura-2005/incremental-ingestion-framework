import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PIPELINE = ROOT / "pipeline.py"
INPUT_FILE = ROOT / "data" / "orders.csv"

if not INPUT_FILE.exists():
    print("ERROR: data/orders.csv not found.")
    sys.exit(1)

start = time.perf_counter()

result = subprocess.run(
    [
        sys.executable,
        str(PIPELINE),
        "--input",
        str(INPUT_FILE),
        "--lookback-minutes",
        "100000000",
    ],
    cwd=ROOT,
    capture_output=True,
    text=True,
)

elapsed = time.perf_counter() - start

print("Exit code:", result.returncode)
print(f"Execution time: {elapsed:.2f} seconds")
print("Pipeline output:")
print(result.stdout)

if result.returncode != 0:
    print("Pipeline error:")
    print(result.stderr)
    sys.exit(result.returncode)

report_dir = ROOT / "reports"
report_dir.mkdir(exist_ok=True)

benchmark_file = report_dir / "benchmark.txt"
benchmark_file.write_text(
    f"Input records: 100000 (generated dataset)\n"
    f"Execution time: {elapsed:.2f} seconds\n"
    f"Exit code: {result.returncode}\n"
    f"Pipeline output:\n{result.stdout}\n",
    encoding="utf-8",
)

print(f"Benchmark saved to: {benchmark_file}")