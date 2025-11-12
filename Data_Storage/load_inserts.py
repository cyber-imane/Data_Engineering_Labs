



# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import csv

# ---------- CONFIGURATION ----------
DBname = "census"
DBuser = "imane2"
DBpwd = "postgres123"    # <-- replace with your actual Postgres password
TableName = 'CensusData'
Datafile = "filedoesnotexist"
CreateDB = False  # indicates whether the DB table should be (re)-created


# ---------- Convert CSV row to SQL value list ----------
def row2vals(row):
    for key in row:
        if row[key] in (None, '', ' '):   # handle empty cells
            row[key] = 0
    row['County'] = row['County'].replace("'", "")  # remove stray quotes

    ret = f"""
       {row['TractId']},            -- TractId
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop
       {row['Men']},                    -- Men
       {row['Women']},                  -- Women
       {row['Hispanic']},               -- Hispanic
       {row['White']},                  -- White
       {row['Black']},                  -- Black
       {row['Native']},                 -- Native
       {row['Asian']},                  -- Asian
       {row['Pacific']},                -- Pacific
       {row['VotingAgeCitizen']},       -- VotingAgeCitizen
       {row['Income']},                 -- Income
       {row['IncomeErr']},              -- IncomeErr
       {row['IncomePerCap']},           -- IncomePerCap
       {row['IncomePerCapErr']},        -- IncomePerCapErr
       {row['Poverty']},                -- Poverty
       {row['ChildPoverty']},           -- ChildPoverty
       {row['Professional']},           -- Professional
       {row['Service']},                -- Service
       {row['Office']},                 -- Office
       {row['Construction']},           -- Construction
       {row['Production']},             -- Production
       {row['Drive']},                  -- Drive
       {row['Carpool']},                -- Carpool
       {row['Transit']},                -- Transit
       {row['Walk']},                   -- Walk
       {row['OtherTransp']},            -- OtherTransp
       {row['WorkAtHome']},             -- WorkAtHome
       {row['MeanCommute']},            -- MeanCommute
       {row['Employed']},               -- Employed
       {row['PrivateWork']},            -- PrivateWork
       {row['PublicWork']},             -- PublicWork
       {row['SelfEmployed']},           -- SelfEmployed
       {row['FamilyWork']},             -- FamilyWork
       {row['Unemployment']}            -- Unemployment
    """
    return ret


# ---------- Parse command-line arguments ----------
def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global Datafile
    Datafile = args.datafile
    global CreateDB
    CreateDB = args.createtable


# ---------- Read CSV file ----------
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r", encoding="utf-8") as fil:
        dr = csv.DictReader(fil)
        rowlist = [row for row in dr]
    print(f"Loaded {len(rowlist)} rows from {fname}")
    return rowlist


# ---------- Convert rows to SQL insert statements ----------
def getSQLcmnds(rowlist):
    cmdlist = []
    for row in rowlist:
        valstr = row2vals(row)
        cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
        cmdlist.append(cmd)
    return cmdlist


# ---------- Connect to PostgreSQL ----------
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = False
    return connection


# ---------- Create table ----------
def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName};
            CREATE TABLE {TableName} (
                TractId             NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                VotingAgeCitizen    DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );
        """)
        print(f"✅ Created {TableName} (no PK or index yet)")



def load(conn, icmdlist):
    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows...")
        start = time.perf_counter()

        for cmd in icmdlist:
            cursor.execute(cmd)
        conn.commit()
        elapsed = time.perf_counter() - start
        print(f"✅ Finished Loading. Elapsed Time: {elapsed:0.4f} seconds")


def addConstraints(conn):
    with conn.cursor() as cursor:
        print("Adding primary key and index...")
        start = time.perf_counter()
        cursor.execute(f"""
            ALTER TABLE {TableName} ADD PRIMARY KEY (TractId);
            CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)
        elapsed = time.perf_counter() - start
        print(f"✅ Constraints and indexes added in {elapsed:0.4f} seconds")





# ---------- Main ----------
def main():
    initialize()
    conn = dbconnect()
    if CreateDB:
        createTable(conn)
    rowlist = readdata(Datafile)
    cmdlist = getSQLcmnds(rowlist)
    load(conn, cmdlist)
    #addConstraints(conn)   # <-- new line for Step 6
    conn.close()

if __name__ == "__main__":
    main()

