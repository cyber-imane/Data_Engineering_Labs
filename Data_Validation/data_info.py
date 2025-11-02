


import os
import pandas as pd

filename = "employees.csv"  # change if needed

# File size
file_size = os.path.getsize(filename)

# Load data
df = pd.read_csv(filename)

print(f"📂 File size: {file_size} bytes")
print(f"🧍 Number of records (rows): {len(df)}")
print(f"🧾 Number of columns: {len(df.columns)}")
print("\n✅ Preview:")
print(df.head())




