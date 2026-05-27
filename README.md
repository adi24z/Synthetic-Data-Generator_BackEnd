# AI Synthetic Data Generator – Backend

## 🧠 Overview

This backend is a **distributed AI-powered synthetic data generation system** that combines:

- FastAPI (API Gateway)
- Apache Airflow (Workflow Orchestration)
- PySpark (Scalable Data Generation)
- Groq LLM (Dynamic Schema Generation)
- Google Cloud Storage (File Storage)
- SMTP Email Service (Notifications)

The system transforms a simple user request into a fully generated dataset pipeline with download and email delivery.

---

# 🏗️ Backend Architecture

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
