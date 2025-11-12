# load_copyfrom.py
# --------------------------------------------------------
# Loads ACS Census data using PostgreSQL's COPY FROM method
# --------------------------------------------------------

import time
import psycopg2
import argparse
import csv

# ---------- CONFIGURATION ----------
DBname = "census"
DBuser = "imane2"
DBpwd = "postgres123"    # your Postgres password
TableName = 'censusdata'  # lowercase for PostgreSQL
Datafile = "filedoesnotexist"
CreateDB = False  # whether to (re)create table


# ---------- Command-line initialization ----------
def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()
    global Datafile, CreateDB
    Datafile = args.datafile
    CreateDB = args.createtable


# ---------- Connect to PostgreSQL ----------
def dbconnect():
    conn = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd
    )
    conn.autocommit = False  # disable autocommit for performance
    return conn


# ---------- Create table (no PK/index yet) ----------
def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName};
            CREATE TABLE {TableName} (
                tractid             NUMERIC,
                state               TEXT,
                county              TEXT,
                totalpop            INTEGER,
                men                 INTEGER,
                women               INTEGER,
                hispanic            DECIMAL,
                white               DECIMAL,
                black               DECIMAL,
                native              DECIMAL,
                asian               DECIMAL,
                pacific             DECIMAL,
                votingagecitizen    DECIMAL,
                income              DECIMAL,
                incomeerr           DECIMAL,
                incomepercap        DECIMAL,
                incomepercaperr     DECIMAL,
                poverty             DECIMAL,
                childpoverty        DECIMAL,
                professional        DECIMAL,
                service             DECIMAL,
                office              DECIMAL,
                construction        DECIMAL,
                production          DECIMAL,
                drive               DECIMAL,
                carpool             DECIMAL,
                transit             DECIMAL,
                walk                DECIMAL,
                othertransp         DECIMAL,
                workathome          DECIMAL,
                meancommute         DECIMAL,
                employed            INTEGER,
                privatework         DECIMAL,
                publicwork          DECIMAL,
                selfemployed        DECIMAL,
                familywork          DECIMAL,
                unemployment        DECIMAL
            );
        """)
        print(f"✅ Created {TableName} (no PK or index yet)")


# ---------- Load data using COPY FROM ----------
def load_using_copy(conn, filename):
    with conn.cursor() as cursor:
        print(f"🚀 Loading data using COPY FROM for file: {filename}")
        start = time.perf_counter()

        with open(filename, 'r', encoding='utf-8') as f:
            next(f)  # skip header
            cursor.copy_from(
                f,
                TableName,
                sep=',',
                null='',
                columns=[
                    'tractid', 'state', 'county', 'totalpop', 'men', 'women',
                    'hispanic', 'white', 'black', 'native', 'asian', 'pacific',
                    'votingagecitizen', 'income', 'incomeerr', 'incomepercap',
                    'incomepercaperr', 'poverty', 'childpoverty', 'professional',
                    'service', 'office', 'construction', 'production', 'drive',
                    'carpool', 'transit', 'walk', 'othertransp', 'workathome',
                    'meancommute', 'employed', 'privatework', 'publicwork',
                    'selfemployed', 'familywork', 'unemployment'
                ]
            )

        conn.commit()
        elapsed = time.perf_counter() - start
        print(f"✅ COPY FROM completed in {elapsed:0.4f} seconds")


# ---------- Add PK and index ----------
def addConstraints(conn):
    with conn.cursor() as cursor:
        print("Adding primary key and index...")
        start = time.perf_counter()
        cursor.execute(f"""
            ALTER TABLE {TableName} ADD PRIMARY KEY (tractid);
            CREATE INDEX idx_{TableName}_state ON {TableName}(state);
        """)
        conn.commit()
        elapsed = time.perf_counter() - start
        print(f"✅ Constraints and indexes added in {elapsed:0.4f} seconds")


# ---------- Validate data ----------
def validate(conn):
    with conn.cursor() as cursor:
        print("\n🔍 Validating loaded data:")
        cursor.execute("SELECT COUNT(DISTINCT state) FROM censusdata;")
        print("Number of states:", cursor.fetchone()[0])

        cursor.execute("SELECT COUNT(DISTINCT county) FROM censusdata WHERE state = 'Oregon';")
        print("Number of Oregon counties:", cursor.fetchone()[0])

        cursor.execute("SELECT COUNT(DISTINCT county) FROM censusdata WHERE state = 'Iowa';")
        print("Number of Iowa counties:", cursor.fetchone()[0])


# ---------- Main driver ----------
def main():
    initialize()
    conn = dbconnect()

    if CreateDB:
        createTable(conn)

    load_using_copy(conn, Datafile)
    addConstraints(conn)
    validate(conn)

    conn.close()


# ---------- Run ----------
if __name__ == "__main__":
    main()

