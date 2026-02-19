from hdbcli import dbapi
from dotenv import load_dotenv
import os

# Loading environment variables
load_dotenv("config/.env")

host = os.getenv("HANA_HOST")
port = int(os.getenv("HANA_PORT"))
user = os.getenv("HANA_USER")
password = os.getenv("HANA_PASSWORD")

try:
    conn = dbapi.connect(
        address=host,
        port=port,
        user=user,
        password=password
    )
    print("Connection to SAP HANA successful!")

    conn.close()

except dbapi.Error as e:
    print ("Connection failed")
    print(e)