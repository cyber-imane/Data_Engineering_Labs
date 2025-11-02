

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

# Load data
df = pd.read_csv("employees.csv")

# Convert birthdate to datetime
df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce")

# Drop missing birthdates
df = df.dropna(subset=["birth_date"])

# Calculate age
today = pd.Timestamp.today()
df["age"] = (today - df["birth_date"]).dt.days // 365

# Plot histogram + smoothed curve
plt.figure(figsize=(8,5))
counts, bins, _ = plt.hist(df["age"], bins=20, density=True, alpha=0.6)

# Simple smoothing to emulate KDE
smoothed = np.convolve(counts, np.ones(3)/3, mode='same')
midpoints = (bins[:-1] + bins[1:]) / 2
plt.plot(midpoints, smoothed)

plt.title("Age Distribution of Employees")
plt.xlabel("Age")
plt.ylabel("Density")
plt.tight_layout()

plt.savefig("age_distribution.png")
plt.close()

print("✅ Age distribution plotted — saved as age_distribution.png")
print(f"Total employees with valid age: {len(df)}")






