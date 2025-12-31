import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "12345")
}


@contextmanager
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def insert_job(job_id, job_name, raw_path):
    sql = """
    INSERT INTO job_metadata (job_id, job_name, status, raw_path)
    VALUES (%s, %s, 'CREATED', %s)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_id, job_name, raw_path))
        conn.commit()

def update_job_status(job_id, status, processed_path=None, error=None):
    sql = """
    UPDATE job_metadata
    SET status = %s,
        processed_path = COALESCE(%s, processed_path),
        error_message = %s,
        updated_at = %s
    WHERE job_id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (status, processed_path, error, datetime.utcnow(), job_id)
            )
        conn.commit()


def fetch_jobs_for_transformation(limit=5):
    sql = """
    SELECT job_id, raw_path
    FROM job_metadata
    WHERE status = 'EXTRACTED'
    FOR UPDATE SKIP LOCKED
    LIMIT %s
    """
    update_sql = """
    UPDATE job_metadata
    SET status = 'RUNNING', updated_at = %s
    WHERE job_id = %s
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            jobs = cur.fetchall()

            for job in jobs:
                cur.execute(update_sql, (datetime.utcnow(), job["job_id"]))

        conn.commit()
        return jobs
