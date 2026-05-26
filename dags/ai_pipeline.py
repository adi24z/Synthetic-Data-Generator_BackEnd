from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pyspark.sql import SparkSession
import pandas as pd
from airflow.operators.python import get_current_context

from faker import Faker
import random
import json
import requests
import os

from utils.gcp_upload import upload_file_to_gcp
from utils.gcp_utils import generate_signed_url
from utils.mailer import send_email

fake = Faker()


# =========================================================
# TASK 1 — GENERATE SCHEMA USING GROQ
# =========================================================

def generate_schema(**context):

    keyword = context["dag_run"].conf.get(
        "keyword",
        "types of Aircrafts"
    )

    prompt = f"""
    Generate realistic dataset columns for:
    {keyword}
    
    Return ONLY valid JSON.

    Example:
    {{
      "columns": [
        "Aircraft_type",
        "Airline_users",
        "Engine_configurations"
      ]
    }}
    """

    response = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {os.getenv('GROQ_API_KEY')}",
            "Content-Type": "application/json"
        },
        json={
            "model": "openai/gpt-oss-120b",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }
    )

    result = response.json()

    print("STATUS CODE:", response.status_code)
    print("FULL RESPONSE:", result)

    if "choices" not in result:
        raise Exception(f"Groq API returned unexpected response: {result}")

    content = result["choices"][0]["message"]["content"]

    schema = json.loads(content)

    print(schema)

    context["ti"].xcom_push(
        key="schema",
        value=schema
    )


# =========================================================
# TASK 2 — GENERATE REALISTIC DATA
# =========================================================

def generate_data(**context):

    schema = context["ti"].xcom_pull(
        key="schema"
    )

    rows = context["dag_run"].conf.get(
        "rows",
        1000
    )

    columns = schema["columns"]

    spark = SparkSession.builder \
        .appName("AIDataGenerator") \
        .master("local[*]") \
        .getOrCreate()

    data = []

    for _ in range(rows):

        row = {}

        for col in columns:

            col_lower = col.lower()

            if "name" in col_lower:
                row[col] = fake.name()

            elif "email" in col_lower:
                row[col] = fake.email()

            elif "phone" in col_lower:
                row[col] = fake.phone_number()

            elif "city" in col_lower:
                row[col] = fake.city()

            elif "address" in col_lower:
                row[col] = fake.address()

            elif "date" in col_lower:
                row[col] = str(fake.date())

            elif "id" in col_lower:
                row[col] = random.randint(1000, 9999)

            else:
                row[col] = fake.word()

        data.append(row)

    pdf = pd.DataFrame(data)

    df = spark.createDataFrame(pdf)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    os.makedirs("/opt/airflow/output", exist_ok=True)

    xml_path = f"/opt/airflow/output/{timestamp}.xml"

    xlsx_path = f"/opt/airflow/output/{timestamp}.xlsx"

    csv_path = f"/opt/airflow/output/{timestamp}.csv"

    # =====================================================
    # XML
    # =====================================================

    pdf.to_xml(
        xml_path,
        root_name="records",
        row_name="record",
        index=False
    )

    # =====================================================
    # EXCEL
    # =====================================================

    pdf.to_excel(
        xlsx_path,
        index=False
    )

    # =====================================================
    # CSV USING SPARK
    # =====================================================

    spark_output = f"/opt/airflow/output/spark_csv_{timestamp}"

    df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .csv(spark_output)

    # Find actual CSV file
    csv_files = [
        f for f in os.listdir(spark_output)
        if f.endswith(".csv")
    ]

    spark_csv_file = os.path.join(
        spark_output,
        csv_files[0]
    )

    import shutil

    shutil.copy(
        spark_csv_file,
        csv_path
    )

    # =====================================================
    # GCP UPLOADS
    # =====================================================

    upload_file_to_gcp(
        xml_path,
        f"xml/{timestamp}.xml"
    )

    upload_file_to_gcp(
        xlsx_path,
        f"excel/{timestamp}.xlsx"
    )

    upload_file_to_gcp(
        csv_path,
        f"csv/{timestamp}.csv"
    )

    # =====================================================
    # SIGNED URL
    # =====================================================

    download_url = generate_signed_url(
        bucket_name= "synthetic_output",
        blob_name = f"csv/{timestamp}.csv"
    )

    print("DOWNLOAD URL:")
    print(download_url)

    context["ti"].xcom_push(
        key="download_url",
        value=download_url
    )

    spark.stop()


# =========================================================
# TASK 3 — SEND EMAIL
# =========================================================

def send_email_task(**context):

    url = context["ti"].xcom_pull(
        key="download_url"
    )

    print("EMAIL SENT")
    print(url)

    send_email(
        recipient=context["dag_run"].conf.get("email", "Something@gmail.com"),
        download_url=url
    )

# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="ai_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="generate_schema",
        python_callable=generate_schema
    )

    task2 = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data
    )

    task3 = PythonOperator(
        task_id="send_email_task",
        python_callable=send_email_task
    )

    task1 >> task2 >> task3

# Get current context:
def generate_schema():

    context = get_current_context()

    conf = context["dag_run"].conf

    keyword = conf.get("keyword")
    rows = conf.get("rows")
    recipient = conf.get("email")

    print(keyword)
    print(rows)
    print(recipient)
