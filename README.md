# AI Synthetic Data Generator – Backend

## Overview

This backend is a **distributed AI-powered synthetic data generation system** that combines:

- FastAPI (API Gateway)
- Apache Airflow (Workflow Orchestration)
- PySpark (Scalable Data Generation)
- Groq LLM (Dynamic Schema Generation)
- Google Cloud Storage (File Storage)
- SMTP Email Service (Notifications)

The system transforms a simple user request into a fully generated dataset pipeline with download and email delivery.

```
User → FastAPI → Airflow DAG → Groq Schema → Spark Data → GCS Storage → Signed URL → Email Delivery
```

---

# Backend Architecture

## High-Level System Design

```text
                    ┌──────────────────────────┐
                    │        Frontend          │
                    │   (React Application)    │
                    └────────────┬─────────────┘
                                 │ REST API
                                 ▼
                    ┌──────────────────────────┐
                    │        FastAPI           │
                    │   Async API Gateway      │
                    └────────────┬─────────────┘
                                 │ DAG Trigger
                                 ▼
        ┌────────────────────────────────────────────────┐
        │              Apache Airflow                   │
        │         (Celery Executor System)              │
        └────────────────────────────────────────────────┘
                 │                │                 │
                 ▼                ▼                 ▼
        ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
        │ Schema Task  │  │ Data Task    │  │ Email Task   │
        │ (Groq LLM)   │  │ (Spark/Faker)│  │ (SMTP Email) │
        └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
               │                 │                 │
               ▼                 ▼                 ▼
        JSON Schema       Synthetic Data    Email Notification
               │                 │                 │
               └───────┬─────────┴─────────┬──────┘
                       ▼                   ▼
             ┌────────────────────────────────────┐
             │     Google Cloud Storage (GCS)    │
             │   CSV / XLSX / XML Outputs        │
             └────────────────────────────────────┘
                              │
                              ▼
                   Signed Download URL (1-hour expiry)
                              │
                              ▼
                      Delivered via Email

```
---
# Process Flow:
## 1) Task Flow
The user submits request via Front-End
```
{
  "keyword": "Aircrafts",
  "rows": 1000,
  "email": "user@gmail.com"
}
```
* Validation of input is done via pydantic
* Airflow DAG execution upon receiving the request
* Endpoints are created to get DAG_ID and DAG_Status

## 2) DAG Trigger (FastAPI -> Airflow)
HTTP (Calling Airflow RESTAPI)
```
POST /api/v1/dags/ai_data_pipeline/dagRuns
```
Payload:
```
{
  "conf": {
    "keyword": "Aircrafts",
    "rows": 1000,
    "email": "user@gmail.com"
  }
}
```
Each request has a separate DAG and and its tagged with a DAG_ID which is system generated

## 3) Schema_Generation (DAG Phase 1)
Generating dataset based on user input in keyword with a predefined prompt hardcoded in the code:
```
keyword: "Aircrafts"
```
```
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
```
#### Schema 
Schema is generated in form of JSON and stored in Airflow XCom which is used for cross communiction between other DAG phases which I will describe below

## 4) Data_Generation (DAG Phase 2)
Takes the output of previous phase through xcom
#### Faker
To create realistic data
| Column Type | Generator       |
| ----------- | --------------- |
| name        | Faker.name()    |
| email       | Faker.email()   |
| city        | Faker.city()    |
| address     | Faker.address() |
| id          | Random integer  |
| default     | Faker.word()    |

#### Pyspark
To create dataframe on the basis of number of rows entered in the front end
```
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
```
#### Pandas
Changing the format of the dataframe 
```
pdf.to_xml(
        xml_path,
        root_name="records",
        row_name="record",
        index=False
    )
```
```
pdf.to_excel(
        xlsx_path,
        index=False
    )
```
#### CSV Output (intended)
```
spark_output = f"/opt/airflow/output/spark_csv_{timestamp}"

    df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .csv(spark_output)
```
## 3) Download URL generation and Email Delivery (DAG Phase 3)
Generated output is stored in GCS bucket 
with the following directories
1) CSV
2) Excel
3) XML
```
csv/{timestamp}.csv
excel/{timestamp}.xlsx
xml/{timestamp}.xml
```

```
gs://synthetic_output/
```

### Signed URL
Every object stored in GCS bucket has a public URL to view (googleapi)
```
def generate_signed_url(bucket_name, blob_name):

    client = storage.Client()

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=1),
        method="GET"
    )

    return url
```
###  Email Delivery
Used Gmail SMTP
```
def send_email(recipient, download_url):

    subject = "Synthetic Dataset Download Link"

    body = f"""
Hello,

Your synthetic dataset has been generated successfully.

Download Link:
{download_url}

This link may expire in 1 hour.

Regards,
AI Data Pipeline
"""

    msg = MIMEMultipart()

    msg["From"] = EMAIL_SENDER
    msg["To"] = recipient
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))
```

## Run Locally
1) git clone "repo URL" in a file
2) Setup ENV variables according to the example
3) Make sure docker desktop is downloaded
4) docker compose build
5) docker compose up -d (Run in detached mode so that terminal is free)
6) Download frontend too and run accordingly
