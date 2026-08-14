"""Create the native read-only Postgres role used by the Databricks ingestion job.

Run after create_lakebase_accounts.py. The password is prompted securely and is
not written to disk. Store the same password in Databricks Secrets afterwards.
"""

from __future__ import annotations

import argparse
import getpass

import psycopg
from psycopg import sql

from lakebase_config import (
    DATABASE,
    HOST,
    INGEST_ROLE,
    PORT,
    admin_connection_kwargs,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Drop/recreate the role if it already exists. Use only for this demo role.",
    )
    return parser.parse_args()


args = parse_args()
password = getpass.getpass(f"Create password for {INGEST_ROLE}: ")
confirm = getpass.getpass("Enter it again: ")
if password != confirm:
    raise SystemExit("Passwords do not match.")
if len(password) < 12:
    raise SystemExit("Use a password of at least 12 characters.")

with psycopg.connect(**admin_connection_kwargs()) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (INGEST_ROLE,))
        exists = cur.fetchone() is not None

        if exists and not args.recreate:
            raise SystemExit(
                f"Role {INGEST_ROLE!r} already exists. Re-run with --recreate only "
                "if you intentionally want to replace this demo role."
            )

        if exists:
            # The demo role should not own objects. DROP OWNED revokes its grants first.
            cur.execute(
                sql.SQL("DROP OWNED BY {}")
                .format(sql.Identifier(INGEST_ROLE))
            )
            cur.execute(
                sql.SQL("DROP ROLE {}")
                .format(sql.Identifier(INGEST_ROLE))
            )

        cur.execute(
            sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD {}")
            .format(sql.Identifier(INGEST_ROLE), sql.Literal(password))
        )
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}")
            .format(sql.Identifier(DATABASE), sql.Identifier(INGEST_ROLE))
        )
        cur.execute(
            sql.SQL("GRANT USAGE ON SCHEMA public TO {}")
            .format(sql.Identifier(INGEST_ROLE))
        )
        cur.execute(
            sql.SQL("GRANT SELECT ON TABLE public.accounts TO {}")
            .format(sql.Identifier(INGEST_ROLE))
        )
    conn.commit()

# Verify the role can log in and read the table.
with psycopg.connect(
    host=HOST,
    port=PORT,
    dbname=DATABASE,
    user=INGEST_ROLE,
    password=password,
    sslmode="require",
    connect_timeout=30,
) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT account_id, account_name, status, updated_at
            FROM public.accounts
            ORDER BY account_id
        """)
        print(f"Connection as {INGEST_ROLE} succeeded:")
        for row in cur.fetchall():
            print(row)

print(f"{INGEST_ROLE} created and verified successfully.")
print("Next: store this username/password in Databricks secret scope metadata-ingestion.")
