import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client():
    return TestClient(app)


# -----------------------------
# Mock Redis cache functions
# -----------------------------
@pytest.fixture(autouse=True)
def mock_redis(monkeypatch):
    store = {}

    def _get_json(key: str):
        return store.get(key)

    # FIXED: Changed 'ttl' to 'ttl_seconds' to match real app code
    def _set_json(key: str, value, ttl_seconds: int):
        store[key] = value

    def _delete(key: str):
        store.pop(key, None)

    monkeypatch.setattr("app.services.redis_cache.cache_get_json", _get_json)
    monkeypatch.setattr("app.services.redis_cache.cache_set_json", _set_json)
    monkeypatch.setattr("app.services.redis_cache.cache_delete", _delete)
    # Routers import cache functions directly; patch their module references too.
    monkeypatch.setattr("app.routers.companies.cache_get_json", _get_json)
    monkeypatch.setattr("app.routers.companies.cache_set_json", _set_json)
    monkeypatch.setattr("app.routers.companies.cache_delete", _delete)
    monkeypatch.setattr("app.routers.assessments.cache_get_json", _get_json)
    monkeypatch.setattr("app.routers.assessments.cache_set_json", _set_json)
    monkeypatch.setattr("app.routers.assessments.cache_delete", _delete)


# -----------------------------
# Mock Snowflake connection
# -----------------------------
class FakeCursor:
    def __init__(self):
        self._one = None
        self._all = []
        self._one_queue = []
        self._all_queue = []
        self.queries = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        return self

    def fetchone(self):
        if self._one_queue:
            return self._one_queue.pop(0)
        return self._one

    def fetchall(self):
        if self._all_queue:
            return self._all_queue.pop(0)
        return self._all

    def close(self):
        pass


class FakeConn:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


@pytest.fixture()
def fake_sf(monkeypatch):
    """
    Gives you a fake cursor/conn you can control per test:
    - cursor._one = (...)  for fetchone
    - cursor._all = [...]  for fetchall
    """
    cursor = FakeCursor()
    conn = FakeConn(cursor)

    monkeypatch.setattr("app.services.snowflake.get_snowflake_connection", lambda: conn)
    # Routers import the function directly; patch their module references too.
    monkeypatch.setattr("app.routers.companies.get_snowflake_connection", lambda: conn)
    monkeypatch.setattr("app.routers.assessments.get_snowflake_connection", lambda: conn)
    return cursor