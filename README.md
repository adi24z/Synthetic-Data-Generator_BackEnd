# AI Synthetic Data Generator – Backend

## Overview

This backend powers an AI-driven synthetic dataset generation pipeline.

It receives a request from the frontend and orchestrates a full workflow using FastAPI + Apache Airflow + Spark + Google Cloud Storage.

---

## Tech Stack

FastAPI (Async API layer)
Apache Airflow 2.9 (Orchestration)
Celery + Redis (Distributed execution, mostly used in YAML)
PostgreSQL (Metadata DB, mostly used in YAML)
PySpark (Data generation)
Pandas (Data formatting)
Groq API (LLM for schema generation)
Google Cloud Storage (File storage)
SMTP (Email notifications)
Docker + Docker Compose

## Run Locally
-> Have docker desktop installed
-> git clone "whatever url is shown for this repo"
-> Set env variables, GROQ API, since i used GCP so create a storage account add a system admin and create gcp key in json format.
-> docker compose build (Build the images on the basis of YAML and dont change the version of dependencies as Airflow is quite strict about it)
-> docker compose up -d (-d because we want to run it in detached mode so that the same terminal remains free and you can run frontend you can see logs in docker desktop, if you want to see logs in cli then dont add -d and use another terminal window to run other things)
-> To see airflow architecture do docker ps

## Architecture

```text
Frontend (React)
      │
      ▼
FastAPI (REST API)
      │
      ▼
Apache Airflow (Celery Executor)
      │
 ┌────┼───────────────┐
 ▼    ▼               ▼
Schema Data        Email Service
Task   Task        Notification
 │      │               │
 ▼      ▼               ▼
Groq  PySpark       SMTP Email
 API   Pandas
      │
      ▼
Google Cloud Storage
      │
      ▼
Signed Download URL


