





# data_integration_lab.py
# Run with: python data_integration_lab.py
# or run cells in a notebook

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------
# 0. Helpful: file names (edit if needed)
cases_file  = "covid_cases.csv"
deaths_file = "covid_deaths.csv"
census_file = "census.csv"
# ------------------------------

# 1. Read the CSVs
cases_df  = pd.read_csv(cases_file, dtype=str)   # read as string initially to avoid surprises
deaths_df = pd.read_csv(deaths_file, dtype=str)
census_df = pd.read_csv(census_file, dtype=str)

# Display initial column names (Step 2.D)
print("cases_df columns:", cases_df.columns.tolist())
print("deaths_df columns:", deaths_df.columns.tolist())
print("census_df columns:", census_df.columns.tolist())

# ------------------------------
# 2. Trim: keep only County Name, State, and 2023-07-23 in cases & deaths
# (lab says 2023-07-23 is the last column and has final cumulative counts)
date_col = "2023-07-23"

# Confirm the date_col exists; if not, list last column
if date_col not in cases_df.columns:
    print("WARNING: expected date column", date_col, "not found in cases_df. Using last column instead:", cases_df.columns[-1])
    date_col_cases = cases_df.columns[-1]
else:
    date_col_cases = date_col

if date_col not in deaths_df.columns:
    print("WARNING: expected date column", date_col, "not found in deaths_df. Using last column instead:", deaths_df.columns[-1])
    date_col_deaths = deaths_df.columns[-1]
else:
    date_col_deaths = date_col

# Keep only the needed columns. Adjust column names if your CSV uses slightly different header names.
# We'll try common alternatives for robustness:
possible_county_cols = ["County Name", "County", "CountyName", "COUNTY_NAME"]
possible_state_cols  = ["State", "state", "STATE", "ST"]

def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    # fallback: choose first column that looks like county/state heuristically
    return None

county_col_cases = find_col(cases_df, possible_county_cols) or cases_df.columns[0]
state_col_cases  = find_col(cases_df, possible_state_cols) or cases_df.columns[1]

county_col_deaths = find_col(deaths_df, possible_county_cols) or deaths_df.columns[0]
state_col_deaths  = find_col(deaths_df, possible_state_cols) or deaths_df.columns[1]

# Trim
cases_df = cases_df[[county_col_cases, state_col_cases, date_col_cases]].copy()
cases_df.columns = ["County Name", "State", date_col_cases]

deaths_df = deaths_df[[county_col_deaths, state_col_deaths, date_col_deaths]].copy()
deaths_df.columns = ["County Name", "State", date_col_deaths]

# Trim census_df
# Keep: County, State, TotalPop, IncomePerCap, Poverty, Unemployment
required_census_cols = ["County", "State", "TotalPop", "IncomePerCap", "Poverty", "Unemployment"]
# find which of these exist (case insensitively)
lower_map = {c.lower(): c for c in census_df.columns}
chosen = {}
for rc in required_census_cols:
    if rc in census_df.columns:
        chosen[rc] = rc
    else:
        # try case-insensitive
        k = rc.lower()
        if k in lower_map:
            chosen[rc] = lower_map[k]
        else:
            # try partial matches (e.g., "Total Pop" or "Total_Pop")
            for col in census_df.columns:
                if rc.lower().replace("percap","") in col.lower().replace("_","").replace(" ",""):
                    chosen[rc] = col
                    break

# Final columns to keep (error if missing)
missing = [rc for rc in required_census_cols if rc not in chosen]
if missing:
    raise ValueError("Missing required census columns: " + ", ".join(missing))

census_df = census_df[[chosen[c] for c in required_census_cols]].copy()
census_df.columns = required_census_cols

print("\nAfter trimming:")
print("cases_df columns:", cases_df.columns.tolist())
print("deaths_df columns:", deaths_df.columns.tolist())
print("census_df columns:", census_df.columns.tolist())

# ------------------------------
# 3. Integration Challenge #1: county names in cases/deaths have trailing space.
# Remove trailing spaces in County Name column in cases & deaths
cases_df["County Name"]  = cases_df["County Name"].astype(str).str.rstrip()
deaths_df["County Name"] = deaths_df["County Name"].astype(str).str.rstrip()

# Test: search for Washington County (without trailing space)
w_cases  = cases_df[cases_df["County Name"] == "Washington County"]
w_deaths = deaths_df[deaths_df["County Name"] == "Washington County"]
print("\n# of rows where County Name == 'Washington County' in cases_df:", len(w_cases))
print("# of rows where County Name == 'Washington County' in deaths_df:", len(w_deaths))

# To count how many counties named "Washington County" total, deduplicate by state as well:
# We'll count unique key (County Name + State)
def count_washington(df):
    return df[df["County Name"] == "Washington County"][["County Name","State"]].drop_duplicates().shape[0]

print("Unique Washington County entries (cases_df):", count_washington(cases_df))
print("Unique Washington County entries (deaths_df):", count_washington(deaths_df))

# ------------------------------
# 4. Integration Challenge #2: remove "Statewide Unallocated" records
for df, name in [(cases_df, "cases_df"), (deaths_df, "deaths_df")]:
    before = len(df)
    df.drop(df[df["County Name"].str.contains("Statewide Unallocated", na=False)].index, inplace=True)
    after = len(df)
    print(f"\n{name}: removed {before-after} 'Statewide Unallocated' rows. Remaining rows: {after}")

# ------------------------------
# 5. Integration Challenge #3: convert state abbreviations to full state names in cases & deaths
# Provide a mapping dict (us_state_abbrev)
us_state_abbrev = {
    'AL': 'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California','CO':'Colorado','CT':'Connecticut',
    'DE':'Delaware','FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa',
    'KS':'Kansas','KY':'Kentucky','LA':'Louisiana','ME':'Maine','MD':'Maryland','MA':'Massachusetts','MI':'Michigan',
    'MN':'Minnesota','MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada','NH':'New Hampshire',
    'NJ':'New Jersey','NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio','OK':'Oklahoma',
    'OR':'Oregon','PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina','SD':'South Dakota','TN':'Tennessee',
    'TX':'Texas','UT':'Utah','VT':'Vermont','VA':'Virginia','WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming',
    # territories if present:
    'DC':'District of Columbia'
}

# Standardize state field: strip whitespace and uppercase if abbreviation present
def expand_state(df):
    s = df["State"].astype(str).str.strip()
    # If it's already full name (contains space or lowercase letters), we'll try to title-case it.
    expanded = []
    for val in s:
        v = val.strip()
        if v.upper() in us_state_abbrev:
            expanded.append(us_state_abbrev[v.upper()])
        else:
            # try to fix common cases: two-letter lowercase -> upper then map
            if len(v) == 2 and v.upper() in us_state_abbrev:
                expanded.append(us_state_abbrev[v.upper()])
            else:
                # assume it's already a full state name; normalize to title case
                expanded.append(v.title())
    df["State"] = expanded

expand_state(cases_df)
expand_state(deaths_df)

# show head of cases_df as requested in lab step 5.B
print("\ncases_df head after state expansion:")
print(cases_df.head())

# ------------------------------
# 6. Integration Challenge #4: create 'key' = County + ", " + State and set as index
for df, name, county_col in [(cases_df, "cases_df", "County Name"),
                             (deaths_df, "deaths_df", "County Name"),
                             (census_df, "census_df", "County")]:
    # Standardize county column name
    if name == "census_df":
        df.rename(columns={"County":"County Name"}, inplace=True)
    # strip
    df["County Name"] = df["County Name"].astype(str).str.strip()
    df["State"] = df["State"].astype(str).str.strip()
    df["key"] = df["County Name"] + ", " + df["State"]
    df.set_index("key", inplace=True)

# Show census_df.head() as requested in 6.C
print("\nCensus df head (after creating key and setting index):")
print(census_df.head())

# ------------------------------
# 7. Integration Challenge #5: rename the date columns to Cases and Deaths
cases_df.rename(columns={date_col_cases: "Cases"}, inplace=True)
deaths_df.rename(columns={date_col_deaths: "Deaths"}, inplace=True)

print("\ncases_df columns now:", cases_df.columns.tolist())
print("deaths_df columns now:", deaths_df.columns.tolist())

# Convert Cases and Deaths to numeric
cases_df["Cases"]  = pd.to_numeric(cases_df["Cases"].str.replace(",",""), errors="coerce").fillna(0).astype(int)
deaths_df["Deaths"] = pd.to_numeric(deaths_df["Deaths"].str.replace(",",""), errors="coerce").fillna(0).astype(int)

# Convert census numeric columns
for col in ["TotalPop","IncomePerCap","Poverty","Unemployment"]:
    census_df[col] = pd.to_numeric(census_df[col].str.replace(",",""), errors="coerce")

# ------------------------------
# 8. Do the integration: join cases_df + deaths_df + census_df
# join on index (key). We'll start with cases_df, join deaths, then census
join_df = cases_df.join(deaths_df[["Deaths"]], how="inner")  # inner join to include common keys
join_df = join_df.join(census_df[["TotalPop","IncomePerCap","Poverty","Unemployment"]], how="inner")

# Compute CasesPerCap and DeathsPerCap
# Avoid division by zero; convert TotalPop to numeric if needed
join_df["TotalPop"] = pd.to_numeric(join_df["TotalPop"], errors="coerce")
join_df["CasesPerCap"] = join_df["Cases"] / join_df["TotalPop"]
join_df["DeathsPerCap"] = join_df["Deaths"] / join_df["TotalPop"]

print("\njoin_df shape (rows, cols):", join_df.shape)
print("First few rows of join_df:")
print(join_df.head())

# ------------------------------
# 9. Analyze: correlation matrix among numeric columns
numeric_cols = ["Cases","Deaths","CasesPerCap","DeathsPerCap","TotalPop","IncomePerCap","Poverty","Unemployment"]
corr = join_df[numeric_cols].corr()
print("\nCorrelation matrix:")
print(corr)

# Save correlation matrix for later use
corr.to_csv("correlation_matrix.csv")

# ------------------------------
# 10. Visualize: heatmap
plt.figure(figsize=(10,8))
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
plt.title('Correlation Matrix Heatmap')
plt.tight_layout()
plt.savefig("correlation_heatmap.png")
plt.show()

# ------------------------------
# 11. Save the joined data
join_df.reset_index(inplace=True)  # bring 'key' back as column
join_df.to_csv("join_df_final.csv", index=False)
print("\nSaved join_df_final.csv and correlation_heatmap.png")

