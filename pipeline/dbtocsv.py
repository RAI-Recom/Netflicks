import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# === Database connection config ===
DB_HOST = "localhost"
DB_PORT = os.getenv('DB_PORT', 5432)
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')

# === Output directory (mounted from local storage) ===
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '.')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "db_backup.csv")

OUTPUT_FILE = "db_backup.csv"

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
    df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 2", conn)
    df['_table_name_'] = table_name
    df = df.fillna('null')

    combined_df = pd.concat([combined_df, df], ignore_index=True)

combined_df = combined_df.fillna('null')

combined_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Data from all tables exported to: {OUTPUT_FILE}")

# === Save to local-mounted CSV file ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
combined_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Data from all tables exported to: {OUTPUT_FILE}")

# === Cleanup ===
cursor.close()
conn.close()