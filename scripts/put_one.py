import os, snowflake.connector
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()

with open(os.environ["SF_PRIVATE_KEY_PATH"], "rb") as f:
    pkey = serialization.load_pem_private_key(f.read(), password=None).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

conn = snowflake.connector.connect(
    account=os.environ["SF_ACCOUNT"],
    user=os.environ["SF_USER"],
    private_key=pkey,
    role=os.environ["SF_ROLE"],
    warehouse=os.environ["SF_WAREHOUSE"],
    database="NYC_TAXI",
    schema="RAW",
)

path = os.path.abspath("yellow_tripdata_2024-01.parquet")
cur = conn.cursor()
cur.execute(f"PUT 'file://{path}' @TLC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=8")
for row in cur.fetchall():
    print(row)
cur.close()
conn.close()