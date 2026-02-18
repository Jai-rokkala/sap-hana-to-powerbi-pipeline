import psycopg2
from dotenv import load_dotenv
import os

load_dotenv("config/.env")

try:
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB")
    )

    print("Connection to PostgreSQL successfull!")

    conn.close()

except psycopg2.Error as e:
    print ("Connection failed")
    print(e)