from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import requests, json
from minio import Minio

# ==========================
# Konfigurasi API & MinIO
# ==========================
API_BASE = "https://trackvisionindo.ddns.net/api2"
USERNAME = "sinarmas"
PASSWORD = "sinar2024"

MINIO_ENDPOINT = "storage.jarfish.ai"
MINIO_BUCKET = "tracking-data"
MINIO_ACCESS_KEY = "YOUR_MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "YOUR_MINIO_SECRET_KEY"


# ==========================
# Step 1: Ambil Token
# ==========================
def get_token(**context):
    url = f"{API_BASE}/token"
    payload = {"username": USERNAME, "password": PASSWORD}
    resp = requests.post(url, json=payload)
    resp.raise_for_status()
    data = resp.json()

    print("DEBUG TOKEN RESPONSE:", data)

    token = data.get("token") or data.get("access_token")
    if not token:
        raise ValueError(f"Token tidak ditemukan dari response! Response={data}")

    context["ti"].xcom_push(key="token", value=token)


# ==========================
# Step 2: Ambil Tracking
# ==========================
def get_tracking(**context):
    token = context["ti"].xcom_pull(key="token", task_ids="get_token")
    run_date = context["ds"]  # format: YYYY-MM-DD

    time_start = f"{run_date} 06:00:00"
    time_end = f"{run_date} 08:00:00"
    lastData_requested = f"{run_date} 06:00:00"

    url = f"{API_BASE}/get-tracking"
    payload = {
        "imei": "868738070027673",
        "time_start": time_start,
        "time_end": time_end,
        "lastData_requested": lastData_requested,
    }
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    print("DEBUG TRACKING RESPONSE:", data)

    accuracy = data.get("accuracy", 0)

    # Validasi akurasi
    if accuracy < 50:
        raise AirflowSkipException(
            f"Akurasi terlalu rendah ({accuracy}), data tidak disimpan ke MinIO"
        )

    # Simpan ke file kalau lolos validasi
    output_file = f"/tmp/tracking_{run_date}.json"
    with open(output_file, "w") as f:
        json.dump(data, f)

    context["ti"].xcom_push(key="tracking_file", value=output_file)


# ==========================
# Step 3: Upload ke MinIO
# ==========================
def upload_minio(**context):
    tracking_file = context["ti"].xcom_pull(
        key="tracking_file", task_ids="get_tracking"
    )
    run_date = context["ds"]

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=True,
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    client.fput_object(MINIO_BUCKET, f"tracking/{run_date}.json", tracking_file)
    print(f"File {tracking_file} berhasil diupload ke MinIO bucket={MINIO_BUCKET}")


# ==========================
# DAG Definition
# ==========================
with DAG(
    dag_id="get_tracking_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 17 * * *",
    catchup=False,
    tags=["tracking", "minio"],
) as dag:

    t1 = PythonOperator(task_id="get_token", python_callable=get_token)

    t2 = PythonOperator(task_id="get_tracking", python_callable=get_tracking)

    t3 = PythonOperator(task_id="upload_minio", python_callable=upload_minio)

    t1 >> t2 >> t3
