import pandas as pd 
import psycopg2
from dotenv import load_dotenv
import os
import logging

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
    # Load simulated SAP data
    df = pd.read_csv("data/sap_oinv.csv", parse_dates=["update_date"])

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB")
    )

    last_loaded = get_last_loaded_time(conn)

    print(f"Last loaded timestamp: {last_loaded}")

    # Incremental filter
    if last_loaded:
        df = df[df["update_date"] > last_loaded]

    print(f"Rows to load: {len(df)}")

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

    logging.info(f"SAP simulated load: {len(df)} rows")
    print("Simulated SAP load completed!")

    cursor.close()
    conn.close()

except Exception as e:
    logging.error(f"SAP simulated ETL failed: {e}")
    print("ETL failed")