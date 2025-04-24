import psycopg2
import pandas as pd

# === Database connection config ===
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "netflicksdb"
DB_USER = "postgres"
DB_PASS = "qweasd"

# === Output file ===
OUTPUT_FILE = "db_backup.csv"

# === Connect to PostgreSQL ===
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cursor = conn.cursor()

# === Get all table names from public schema ===
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';
""")
tables = cursor.fetchall()

# === Combine all tables ===
combined_df = pd.DataFrame()

for (table_name,) in tables:
    print(f"Loading: {table_name}")
    df = pd.read_sql_query(f"SELECT * FROM {table_name} limit 20", conn)
    df['__table_name__'] = table_name  # Optional: track source table
    combined_df = pd.concat([combined_df, df], ignore_index=True)

# === Save to single CSV file ===
combined_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Data from all tables exported to: {OUTPUT_FILE}")

# === Cleanup ===
cursor.close()
conn.close()
