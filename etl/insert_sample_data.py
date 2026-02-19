import psycopg2
from dotenv import load_dotenv
import os

load_dotenv("config/.env")

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    dbname=os.getenv("PG_DB")
)

cursor = conn.cursor()

query = """
INSERT INTO stg_invoices 
(doc_entry, doc_num, card_code, doc_date, doc_total, update_date)
VALUES (%s, %s, %s, %s, %s, NOW())
ON CONFLICT (doc_entry) DO NOTHING;
"""

data = (2, 1002, "C002", "2026-01-02", 7500)

cursor.execute(query, data)
conn.commit()

print("Data inserted successfully!")

cursor.close()
conn.close()