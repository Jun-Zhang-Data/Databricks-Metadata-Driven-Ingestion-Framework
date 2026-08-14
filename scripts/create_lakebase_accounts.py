"""Create/reset the initial Lakebase source table used by the JDBC demo."""

import psycopg

from lakebase_config import admin_connection_kwargs


with psycopg.connect(**admin_connection_kwargs()) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.accounts (
                account_id INTEGER PRIMARY KEY,
                account_name VARCHAR(100) NOT NULL,
                status VARCHAR(30) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
        """)
        cur.execute("TRUNCATE TABLE public.accounts")
        cur.execute("""
            INSERT INTO public.accounts
                (account_id, account_name, status, updated_at)
            VALUES
                (1, 'Nordic Retail', 'ACTIVE',   '2026-08-13T19:00:00Z'),
                (2, 'Blue Logistics', 'ACTIVE', '2026-08-13T20:00:00Z'),
                (3, 'Stockholm Foods', 'INACTIVE', '2026-08-13T21:00:00Z')
        """)
        cur.execute("""
            SELECT account_id, account_name, status, updated_at
            FROM public.accounts
            ORDER BY account_id
        """)
        for row in cur.fetchall():
            print(row)
    conn.commit()

print("Lakebase accounts table created successfully.")
