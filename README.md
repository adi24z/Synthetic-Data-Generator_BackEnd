# AI Synthetic Data Generator – Backend

## Overview

This backend powers an AI-driven synthetic dataset generation pipeline.

It receives a request from the frontend and orchestrates a full workflow using FastAPI + Apache Airflow + Spark + Google Cloud Storage.

---

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



