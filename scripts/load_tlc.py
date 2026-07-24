import os
import sys
import tempfile
from datetime import date

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def target_month() -> str:
    """TLC publishes ~2 months behind. TARGET_MONTH overrides for backfills."""
    override = os.environ.get("TARGET_MONTH", "").strip()
    if override:
        return override
    today = date.today()
    m, y = today.month - 2, today.year
    if m <= 0:
        m += 12
        y -= 1
    return f"{y}-{m:02d}"


def download(month: str) -> str:
    fname = f"yellow_tripdata_{month}.parquet"
    url = f"{BASE_URL}/{fname}"
    path = os.path.join(tempfile.gettempdir(), fname)

    print(f"Downloading {url}")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)

    print(f"Saved {path} ({os.path.getsize(path) / 1e6:.1f} MB)")
    return path


def private_key() -> bytes:
    """Local dev uses a file path; GitHub Actions passes the key contents."""
    key_path = os.environ.get("SF_PRIVATE_KEY_PATH")
    if key_path:
        with open(key_path, "rb") as f:
            raw = f.read()
    else:
        raw = os.environ["SF_PRIVATE_KEY"].encode()

    return serialization.load_pem_private_key(raw, password=None).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect():
    return snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        private_key=private_key(),
        role=os.environ.get("SF_ROLE", "SYSADMIN"),
        warehouse=os.environ.get("SF_WAREHOUSE", "LOAD_WH"),
        database="NYC_TAXI",
        schema="RAW",
    )


COPY_SQL = """
COPY INTO RAW_YELLOW_TRIPS
  (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
   passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
   PULocationID, DOLocationID, payment_type, fare_amount, extra,
   mta_tax, tip_amount, tolls_amount, improvement_surcharge,
   total_amount, congestion_surcharge, Airport_fee, _source_file)
FROM (
  SELECT
    $1:VendorID,
    TO_TIMESTAMP_NTZ($1:tpep_pickup_datetime::NUMBER, 6),
    TO_TIMESTAMP_NTZ($1:tpep_dropoff_datetime::NUMBER, 6),
    $1:passenger_count, $1:trip_distance, $1:RatecodeID,
    $1:store_and_fwd_flag, $1:PULocationID, $1:DOLocationID,
    $1:payment_type, $1:fare_amount, $1:extra, $1:mta_tax,
    $1:tip_amount, $1:tolls_amount, $1:improvement_surcharge,
    $1:total_amount, $1:congestion_surcharge,
    COALESCE($1:Airport_fee, $1:airport_fee),
    METADATA$FILENAME
  FROM @TLC_STAGE
)
FILES = ('{fname}')
FILE_FORMAT = (FORMAT_NAME = PARQUET_FF)
ON_ERROR = ABORT_STATEMENT
"""


def main():
    month = target_month()
    print(f"Target month: {month}")

    path = download(month)
    fname = os.path.basename(path)

    conn = connect()
    cur = conn.cursor()
    try:
        posix = path.replace("\\", "/")
        cur.execute(
            f"PUT 'file://{posix}' @TLC_STAGE "
            "AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = 8"
        )
        print("PUT:", cur.fetchall())

        cur.execute(COPY_SQL.format(fname=fname))
        print("COPY:", cur.fetchall())

        cur.execute(f"REMOVE @TLC_STAGE/{fname}")

        cur.execute(
            "SELECT COUNT(*) FROM RAW_YELLOW_TRIPS WHERE _source_file LIKE %s",
            (f"%{fname}",),
        )
        print(f"Rows loaded for {month}: {cur.fetchone()[0]:,}")
    finally:
        cur.close()
        conn.close()
        if os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    sys.exit(main())