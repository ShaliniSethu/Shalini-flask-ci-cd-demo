from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request, g

app = Flask(__name__)

ALLOWED_STATUSES = {"pending", "running", "done", "failed"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_error(message: str, status_code: int = 400):
    return jsonify({"error": message}), status_code

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            job_type TEXT,
            created_by TEXT,
            status TEXT NOT NULL,
            payload TEXT,
            result TEXT,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # Lightweight migration for existing DBs (adds missing columns)
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}

    if "job_type" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN job_type TEXT")
    if "created_by" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN created_by TEXT")
    conn.commit()


def get_db_path() -> str:
    """
    Default DB file is tasks.db in project folder.
    Override with environment variable TASKS_DB_PATH (useful for tests / Docker volumes).
    """
    return os.environ.get("TASKS_DB_PATH", "tasks.db")


def get_db() -> sqlite3.Connection:
    """
    One SQLite connection per request (stored in Flask g).
    Ensures schema exists for whichever DB path is active.
    """
    if "db" not in g:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()




def row_to_task(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "job_type": row["job_type"],
        "created_by": row["created_by"],
        "status": row["status"],
        "payload": row["payload"],
        "result": row["result"],
        "error": row["error"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


@app.get("/")
def home():
    return jsonify(message="Job Orchestrator is running, routes available /health, /tasks, /tasks/<task_id>"), 200


@app.get("/health")
def health():
    return jsonify(status="ok"), 200


@app.post("/tasks")
def create_task():
    if not request.is_json:
        return make_error("Request body must be JSON", 400)

    data = request.get_json(silent=True) or {}
    name = data.get("name")
    job_type = data.get("job_type")
    created_by = data.get("created_by")
    payload = data.get("payload")

    if not isinstance(name, str) or not name.strip():
        return make_error("Field 'name' is required and must be a non-empty string", 400)
    
    if job_type is not None and (not isinstance(job_type, str) or not job_type.strip()):
        return make_error("Field 'job_type', if provided, must be a non-empty string", 400)
    
    if created_by is not None and (not isinstance(created_by, str) or not created_by.strip()):
        return make_error("Field 'created_by' must be a non-empty string if provided", 400)
    
    task_id = str(uuid4())
    now = utc_now_iso()

    # Store payload/result/error as strings for simplicity (dependency-free).
    # If payload is JSON, we'll store it as a string representation.
    payload_str = None if payload is None else str(payload)

    db = get_db()
    db.execute(
        """
        INSERT INTO tasks (id, name, job_type, created_by, status, payload, result, error, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        
        (task_id, name.strip(), job_type.strip() if isinstance(job_type, str) else None, created_by.strip() if isinstance(created_by, str) else None, "pending", payload_str, None, None, now, now),
    )
    db.commit()

    created = db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    return jsonify(row_to_task(created)), 201


@app.get("/tasks")
def list_tasks():
    status = request.args.get("status")

    if status is not None and status not in ALLOWED_STATUSES:
        return make_error(f"Invalid status filter. Allowed: {sorted(ALLOWED_STATUSES)}", 400)

    db = get_db()
    if status is None:
        rows = db.execute(
            "SELECT * FROM tasks ORDER BY created_at DESC"
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM tasks WHERE status = ? ORDER BY created_at DESC",
            (status,),
        ).fetchall()

    return jsonify([row_to_task(r) for r in rows]), 200


@app.get("/tasks/<task_id>")
def get_task(task_id: str):
    db = get_db()
    row = db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        return make_error("Task not found", 404)
    return jsonify(row_to_task(row)), 200


@app.patch("/tasks/<task_id>")
def update_task(task_id: str):
    if not request.is_json:
        return make_error("Request body must be JSON", 400)

    data = request.get_json(silent=True) or {}
    status = data.get("status")
    result = data.get("result")
    error = data.get("error")

    if status is None:
        return make_error("Field 'status' is required", 400)
    if status not in ALLOWED_STATUSES:
        return make_error(f"Invalid status. Allowed: {sorted(ALLOWED_STATUSES)}", 400)

    if status == "failed" and (not isinstance(error, str) or not error.strip()):
        return make_error("Field 'error' is required when status is 'failed'", 400)

    db = get_db()
    existing = db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if existing is None:
        return make_error("Task not found", 404)

    now = utc_now_iso()

    # Simple state rules
    if status == "done":
        result_str = None if result is None else str(result)
        db.execute(
            """
            UPDATE tasks
            SET status = ?, result = ?, error = NULL, updated_at = ?
            WHERE id = ?
            """,
            (status, result_str, now, task_id),
        )
    elif status == "failed":
        db.execute(
            """
            UPDATE tasks
            SET status = ?, error = ?, result = NULL, updated_at = ?
            WHERE id = ?
            """,
            (status, error.strip(), now, task_id),
        )
    else:
        db.execute(
            """
            UPDATE tasks
            SET status = ?, result = NULL, error = NULL, updated_at = ?
            WHERE id = ?
            """,
            (status, now, task_id),
        )

    db.commit()
    updated = db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    return jsonify(row_to_task(updated)), 200


@app.delete("/tasks/<task_id>")
def delete_task(task_id: str):
    db = get_db()
    cur = db.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    db.commit()

    if cur.rowcount == 0:
        return make_error("Task not found", 404)

    return ("", 204)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
