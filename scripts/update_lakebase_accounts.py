"""Create the incremental Lakebase batch for the JDBC MERGE + watermark demo."""

import psycopg

from lakebase_config import admin_connection_kwargs


with psycopg.connect(**admin_connection_kwargs()) as conn:
    with conn.cursor() as cur:
        # Existing key -> should become a MERGE UPDATE after watermark filtering.
        cur.execute("""
            UPDATE public.accounts
            SET status = 'INACTIVE',
                updated_at = '2026-08-13T22:00:00Z'
            WHERE account_id = 2
        """)

        # New key -> should become a MERGE INSERT.
        cur.execute("""
            INSERT INTO public.accounts
                (account_id, account_name, status, updated_at)
            VALUES
                (4, 'Gothenburg Tech', 'ACTIVE', '2026-08-13T23:00:00Z')
            ON CONFLICT (account_id) DO UPDATE SET
                account_name = EXCLUDED.account_name,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
        """)

        cur.execute("""
            SELECT account_id, account_name, status, updated_at
            FROM public.accounts
            ORDER BY account_id
        """)
        for row in cur.fetchall():
            print(row)
    conn.commit()

print("Incremental Lakebase accounts batch created.")
