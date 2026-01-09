# pyright: reportUnusedFunction=false

import os
import tempfile
import pytest

from app import app  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_db_between_tests():
    client = app.test_client()
    # delete all tasks by direct SQL via app context
    with app.app_context():
        from app import get_db  # local import to avoid circular issues
        db = get_db()
        db.execute("DELETE FROM tasks")
        db.commit()
    yield


def test_home():
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200



def test_health():
    client = app.test_client()
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}


def test_create_task_returns_201_and_task_shape():
    client = app.test_client()
    resp = client.post("/tasks", json={"name": "backup-db", "job-type": "db-backup", "payload": {"db": "prod"}})
    assert resp.status_code == 201

    task = resp.get_json()
    assert isinstance(task["id"], str) and task["id"]
    assert task["name"] == "backup-db"
    assert task["job_type"] == "db-backup"
    assert task["status"] == "pending"
    # payload is stored as string in this minimal SQLite version
    assert "prod" in (task["payload"] or "")
    assert task["result"] is None
    assert task["error"] is None
    assert "created_at" in task
    assert "updated_at" in task


def test_list_tasks_contains_created_task():
    client = app.test_client()
    created = client.post("/tasks", json={"name": "task-1"})
    task_id = created.get_json()["id"]

    resp = client.get("/tasks")
    assert resp.status_code == 200
    tasks = resp.get_json()
    assert any(t["id"] == task_id for t in tasks)


def test_get_task_returns_404_for_unknown_id():
    client = app.test_client()
    resp = client.get("/tasks/not-a-real-id")
    assert resp.status_code == 404


def test_update_task_status_running_then_done():
    client = app.test_client()
    created = client.post("/tasks", json={"name": "compile"})
    task_id = created.get_json()["id"]

    running = client.patch(f"/tasks/{task_id}", json={"status": "running"})
    assert running.status_code == 200
    assert running.get_json()["status"] == "running"


    done = client.patch(f"/tasks/{task_id}", json={"status": "done", "result": {"took_seconds": 2.3}})
    assert done.status_code == 200
    body = done.get_json()
    assert body["status"] == "done"
    assert "2.3" in (body["result"] or "")  # stored as string


def test_update_task_failed_requires_error():
    client = app.test_client()
    created = client.post("/tasks", json={"name": "deploy"})
    task_id = created.get_json()["id"]

    resp = client.patch(f"/tasks/{task_id}", json={"status": "failed"})
    assert resp.status_code == 400


def test_update_task_rejects_invalid_status():
    client = app.test_client()
    created = client.post("/tasks", json={"name": "lint"})
    task_id = created.get_json()["id"]

    resp = client.patch(f"/tasks/{task_id}", json={"status": "weird"})
    assert resp.status_code == 400


def test_delete_task_removes_it():
    client = app.test_client()
    created = client.post("/tasks", json={"name": "clean"})
    task_id = created.get_json()["id"]

    deleted = client.delete(f"/tasks/{task_id}")
    assert deleted.status_code == 204

    get_again = client.get(f"/tasks/{task_id}")
    assert get_again.status_code == 404


def test_filter_tasks_by_status():
    client = app.test_client()
    t1 = client.post("/tasks", json={"name": "backup1", "job-type": "backup", "payload": {"db": "dev"}}).get_json()["id"]
    t2 = client.post("/tasks", json={"name": "backup2", "job-type": "backup", "payload": {"db": "test"}}).get_json()["id"]

    client.patch(f"/tasks/{t1}", json={"status": "running"})

    resp = client.get("/tasks?status=running")
    assert resp.status_code == 200
    tasks = resp.get_json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == t1

    resp2 = client.get("/tasks?status=pending")
    assert resp2.status_code == 200
    tasks2 = resp2.get_json()
    assert len(tasks2) == 1
    assert tasks2[0]["id"] == t2
