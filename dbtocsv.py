import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = "localhost"
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')

# === Output directory ===
OUTPUT_DIR = "/home/Recomm-project/datav"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "data_backup.csv")

def export_all_tables_to_csv():
    

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

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
        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 15000", conn)
        df['table_name'] = table_name  # Optional: track source table

        # Replace all actual NaN/None values with the string 'null'
        df = df.fillna('null')

        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # === Replace NaN/nulls in the full combined dataframe ===
    combined_df = combined_df.fillna('null')

    # === Save to single CSV file ===
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Data from all tables exported to: {OUTPUT_FILE}")

    # === Save to local-mounted CSV file ===
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # create folder if missing
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Data from all tables exported to: {OUTPUT_FILE}")


    # === Cleanup ===
    cursor.close()
    conn.close()

if __name__ == "__main__":
    export_all_tables_to_csv()