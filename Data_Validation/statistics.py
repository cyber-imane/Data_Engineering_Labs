
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load dataset
df = pd.read_csv("employees.csv")

# Convert salary to numeric and clean
df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
df = df.dropna(subset=["salary", "title"])

# Get top 6 job titles by employee count
top_jobs = df["title"].value_counts().nlargest(6).index
print("Top job titles being plotted:", list(top_jobs))

def smooth_curve(y, window=3):
    """Simple moving average smoothing since no scipy available."""
    return np.convolve(y, np.ones(window)/window, mode='same')

for job in top_jobs:
    salaries = df[df["title"] == job]["salary"]

    plt.figure(figsize=(8,5))

    # Histogram data
    counts, bins, _ = plt.hist(salaries, bins=20, alpha=0.6, density=True)

    # Smooth curve
    smoothed = smooth_curve(counts, window=3)
    midpoints = (bins[:-1] + bins[1:]) / 2
    plt.plot(midpoints, smoothed)

    plt.title(f"Salary Distribution: {job}")
    plt.xlabel("Salary")
    plt.ylabel("Density")
    plt.tight_layout()

    filename = f"salary_hist_{job.replace(' ', '_')}.png"
    plt.savefig(filename)
    plt.close()
    print(f"✅ Saved: {filename}")

print("\n🎉 Done! Salary histograms generated for top job titles.")

