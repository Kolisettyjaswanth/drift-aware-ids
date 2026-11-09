
from pathlib import Path
import csv

DATA_DIR = Path("data/raw/cicids2017")

files = list(DATA_DIR.rglob("*PortScan*.csv"))

if len(files) != 1:
    raise FileNotFoundError(
        f"Expected one PortScan CSV, found {len(files)}"
    )

file_path = files[0]

with file_path.open(
    encoding="utf-8-sig",
    errors="replace",
    newline=""
) as file:
    reader = csv.reader(file)

    columns = next(reader)
    first_record = next(reader)

print("Dataset:", file_path.name)
print("Total columns:", len(columns))

print("\nFirst 15 features:")
for feature in columns[:15]:
    print("-", feature.strip())

print("\nFirst network-flow record:")
for feature, value in zip(columns[:15], first_record[:15]):
    print(f"{feature.strip()}: {value.strip()}")

print("\nTraffic label:", first_record[-1].strip())
