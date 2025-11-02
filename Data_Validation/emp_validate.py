import pandas as pd

filename = "employees.csv"  # adjust if needed

# Load dataset
df = pd.read_csv(filename)

print("=== Employee Data Validation ===")
print(f"Total records: {len(df)}")

# --------------------------
# 1) Existence Assertion: name must not be null
# --------------------------
missing_name = df['name'].isna().sum()
print(f"Records missing 'name': {missing_name}")

# --------------------------
# GRAD: Existence Assertion #2: eid must not be null
# --------------------------
missing_eid = df['eid'].isna().sum()
print(f"Records missing 'eid': {missing_eid}")

# --------------------------
# 2) Limit Assertion: hire date must be >= 2015
# --------------------------
df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')
violations_hire_date = (df['hire_date'] < pd.Timestamp("2015-01-01")).sum()
print(f"Records hired before 2015: {violations_hire_date}")

# --------------------------
# GRAD Limit Assertion #2:
# Salary must be greater than $80,000
# --------------------------
if 'salary' in df.columns:
    df['salary'] = pd.to_numeric(df['salary'], errors='coerce')
    violations_salary = (df['salary'] <= 80000).sum()
else:
    violations_salary = "N/A — salary column not found"

print(f"Records with salary <= $80,000: {violations_salary}")


# --------------------------
# 3) Intra-record Assertion: birth_date < hire_date
# --------------------------
df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')

violations_birth_vs_hire = (df['birth_date'] >= df['hire_date']).sum()
print(f"Birth date AFTER hire date violations: {violations_birth_vs_hire}")


# --------------------------
# GRAD: Intra-record Assertion
# Salary should align with job level
# --------------------------

violations_salary_vs_level = 0

if 'salary' in df.columns and 'title' in df.columns:

    # Convert to numeric
    df['salary'] = pd.to_numeric(df['salary'], errors='coerce')

    # Rules based on level
    conditions = (
        ((df['title'] == 'Executive') & (df['salary'] < 150000)) |
        ((df['title'] == 'Manager') & (df['salary'] < 90000)) |
        ((df['title'] == 'Analyst') & (df['salary'] < 60000))
    )

    violations_salary_vs_level = conditions.sum()
    print(f"Salary inconsistent with job level: {violations_salary_vs_level}")

else:
    print("⚠️ job_level or annual_salary column not found — skipping job-level salary rule")





# Convert reports_to to numeric (in case of blanks/strings)
df['reports_to'] = pd.to_numeric(df['reports_to'], errors='coerce')

# Set of existing employee IDs
valid_eids = set(df['eid'].dropna())

# Employees who have manager ID but that ID doesn't exist in employee list
invalid_manager_refs = df[
    (df['reports_to'].notna()) & (~df['reports_to'].isin(valid_eids))
]

print(f"Employees with unknown manager (invalid reports_to): {len(invalid_manager_refs)}")

# --------------------------
# GRAD Inter-record Assertion:
# Each phone number must have exactly 10 digits
# --------------------------

if 'phone' in df.columns:
    # Remove non-digits
    df['digits_only_phone'] = df['phone'].astype(str).str.replace(r'\D', '', regex=True)
    
    invalid_phone_count = (df['digits_only_phone'].str.len() != 10).sum()
else:
    invalid_phone_count = "N/A — phone column not found"

print(f"Invalid phone numbers (not 10 digits): {invalid_phone_count}")




# --------------------------
# 5) Summary Assertion:
# Each city must have more than one employee
# --------------------------

if 'city' in df.columns:
    city_counts = df['city'].value_counts()
    cities_with_one = city_counts[city_counts <= 1]  # <=1 in case any city has zero theoretically

    print(f"Cities with only one (or zero) employee: {len(cities_with_one)}")
    print("Dataset valid for city rule?" , "YES ✅" if len(cities_with_one) == 0 else "NO ❌")
else:
    print("⚠️ 'city' column not found — skipping city summary check")



# --------------------------
# GRAD Summary Assertion:
# Each job title must have at least 3 employees
# --------------------------

if 'title' in df.columns:
    title_counts = df['title'].value_counts()
    small_titles = title_counts[title_counts < 3]

    print(f"Job titles with fewer than 3 employees: {len(small_titles)}")
    print("Dataset valid for job title rule?" , "YES ✅" if len(small_titles) == 0 else "NO ❌")
else:
    print("⚠️ 'title' column not found — skipping title summary check")


