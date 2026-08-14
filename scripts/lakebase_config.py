"""Non-secret Lakebase demo configuration for Project 2.

Workspace/user-specific values are supplied through environment variables.
Database passwords are never stored here.

Required for the Lakebase helper scripts:
- P2_LAKEBASE_HOST
- P2_LAKEBASE_ADMIN_USER

Optional overrides:
- DATABRICKS_CLI
- DATABRICKS_PROFILE
- P2_LAKEBASE_PROJECT
- P2_LAKEBASE_BRANCH
- P2_LAKEBASE_ENDPOINT
- P2_LAKEBASE_PORT
- P2_LAKEBASE_DATABASE
- P2_LAKEBASE_INGEST_ROLE
"""

from __future__ import annotations

import json
import os
import subprocess


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Environment variable {name} is required. "
            "See scripts/set_env.example.ps1 and scripts/README.md."
        )
    return value


CLI = os.getenv("DATABRICKS_CLI", "databricks").strip() or "databricks"
PROFILE = os.getenv("DATABRICKS_PROFILE", "").strip()

PROJECT_ID = os.getenv("P2_LAKEBASE_PROJECT", "project2-jdbc")
BRANCH_ID = os.getenv("P2_LAKEBASE_BRANCH", "production")
ENDPOINT_ID = os.getenv("P2_LAKEBASE_ENDPOINT", "primary")

ENDPOINT_RESOURCE = (
    f"projects/{PROJECT_ID}/branches/{BRANCH_ID}/endpoints/{ENDPOINT_ID}"
)

HOST = required_env("P2_LAKEBASE_HOST")
PORT = int(os.getenv("P2_LAKEBASE_PORT", "5432"))
DATABASE = os.getenv("P2_LAKEBASE_DATABASE", "databricks_postgres")
ADMIN_USER = required_env("P2_LAKEBASE_ADMIN_USER")
INGEST_ROLE = os.getenv("P2_LAKEBASE_INGEST_ROLE", "p2_ingest")


def generate_admin_oauth_token() -> str:
    """Generate a short-lived Lakebase database credential through Databricks CLI."""
    command = [
        CLI,
        "postgres",
        "generate-database-credential",
        ENDPOINT_RESOURCE,
    ]

    if PROFILE:
        command.extend(["--profile", PROFILE])

    command.extend(["--output", "json"])

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    token = payload.get("token")
    if not token:
        raise RuntimeError("Databricks CLI response did not contain a database token")
    return token


def admin_connection_kwargs() -> dict:
    return {
        "host": HOST,
        "port": PORT,
        "dbname": DATABASE,
        "user": ADMIN_USER,
        "password": generate_admin_oauth_token(),
        "sslmode": "require",
        "connect_timeout": 30,
    }
