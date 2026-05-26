from fastapi import FastAPI
from pydantic import BaseModel, EmailStr
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI()

AIRFLOW_BASE_URL = "http://airflow-webserver:8080"
AUTH = ("airflow", "airflow")


class RequestData(BaseModel):
    keyword: str
    rows: int
    email: EmailStr


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/generate-data")
async def generate_data(data: RequestData):

    async with httpx.AsyncClient() as client:

        response = await client.post(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/ai_data_pipeline/dagRuns",
            auth=AUTH,
            json={
                "conf": {
                    "keyword": data.keyword,
                    "rows": data.rows,
                    "email": data.email
                }
            }
        )

    response.raise_for_status()

    dag_response = response.json()

    return {
        "message": "Pipeline Triggered",
        "dag_run_id": dag_response["dag_run_id"],
        "state": dag_response["state"]
    }


@app.get("/job-status/{dag_run_id}")
async def job_status(dag_run_id: str):

    async with httpx.AsyncClient() as client:

        response = await client.get(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/ai_data_pipeline/dagRuns/{dag_run_id}",
            auth=AUTH
        )

    response.raise_for_status()

    return response.json()


@app.get("/task-status/{dag_run_id}")
async def task_status(dag_run_id: str):

    async with httpx.AsyncClient() as client:

        response = await client.get(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/ai_data_pipeline/dagRuns/{dag_run_id}/taskInstances",
            auth=AUTH
        )

    response.raise_for_status()

    return response.json()