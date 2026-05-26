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


