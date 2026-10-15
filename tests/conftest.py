import pytest


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setattr("lakehouse.config.ROOT", tmp_path)
    return tmp_path
