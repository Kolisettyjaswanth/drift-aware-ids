
from pathlib import Path
from collections import Counter
import pandas as pd

data_dir = Path("data/raw/cicids2017")

files = list(data_dir.rglob("*PortScan*.csv"))

if len(files) != 1:
    raise FileNotFoundError(
        f"Expected one PortScan CSV, found {len(files)}"
    )

file_path = files[0]

# Read only the header to locate the label column.
columns = pd.read_csv(file_path, nrows=0).columns
label_columns = [
    col for col in columns
    if col.strip() == "Label"
]

if len(label_columns) != 1:
    raise ValueError("Expected exactly one Label column")

label_column = label_columns[0]

counts = Counter()
total_rows = 0

# Read 50,000 rows at a time.
for chunk in pd.read_csv(
    file_path,
    usecols=[label_column],
    chunksize=50_000,
    encoding="utf-8-sig",
):
    labels = chunk[label_column].astype("string").str.strip()

    counts.update(labels.fillna("<MISSING>").tolist())
    total_rows += len(chunk)

print("File:", file_path.name)
print("Total records:", total_rows)

print("\nTraffic distribution:")
for label, count in counts.most_common():
    percentage = (count / total_rows) * 100
    print(f"{label}: {count:,} ({percentage:.2f}%)")
