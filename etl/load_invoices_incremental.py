import pandas as pd
from dotenv import load_dotenv
import os
import logging
import psycopg2
from datetime import datetime

#loading .env
load_dotenv("config/.env")

logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_last_loaded_time(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(update_date) FROM stg_invoices;")
    result = cursor.fetchone()[0]
    cursor.close()
    return result

try:
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB")
    )
    last_loaded = get_last_loaded_time(conn)

    print(f"Last loaded timestamp: {last_loaded}")

    # 🔹 Simulated NEW SAP data
    now = datetime.now()

    data = {
        "doc_entry": [20, 21],
        "doc_num": [2020, 2021],
        "card_code": ["C020", "C021"],
        "doc_date": ["2026-02-01", "2026-02-02"],
        "doc_total": [4000, 5000],
        "update_date": [now, now]
    }

    df = pd.DataFrame(data)

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO stg_invoices 
    (doc_entry, doc_num, card_code, doc_date, doc_total, update_date)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (doc_entry) DO NOTHING;
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    conn.commit()

    logging.info(f"Incremental load: {len(df)} rows")
    print("Incremental ETL completed!")

    cursor.close()
    conn.close()

except Exception as e:
    logging.error(f"Incremental ETL failed: {e}")
    print("❌ Incremental ETL failed")