import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import logging

#loading .env
load_dotenv("config/.env")

#logging setup
logging.basicConfig(
    filename="logs/load_invoices.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
    )

try:
    # 🔹 Simulated SAP data
    data = {
        "doc_entry": [10, 11, 12],
        "doc_num": [1010, 1011, 1012],
        "card_code": ["C010", "C011", "C012"],
        "doc_date": ["2026-01-10", "2026-01-11", "2026-01-12"],
        "doc_total": [1000, 2000, 3000]
    }

    df = pd.DataFrame(data)

    # DB connection
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB")
    )

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO stg_invoices 
    (doc_entry, doc_num, card_code, doc_date, doc_total, update_date)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (doc_entry) DO NOTHING;
    """

    # Bulk insert
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    conn.commit()

    logging.info(f"Loaded {len(df)} rows successfully")
    print("ETL completed!")

    cursor.close()
    conn.close()

except Exception as e:
    logging.error(f"ETL failed: {e}")
    print("ETL failed")