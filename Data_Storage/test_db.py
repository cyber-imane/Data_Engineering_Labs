# Save this as test_db.py
import psycopg2

try:
    conn = psycopg2.connect(
        dbname="census",
        user="imane2",
        password="postgres123",
        host="localhost"
    )
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)

