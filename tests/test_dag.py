import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAG_FILE = Path(__file__).parents[1] / "dags" / "card_transactions_dag.py"


def test_dag_structure():
    spec = importlib.util.spec_from_file_location("card_transactions_dag", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dag = module.card_transactions_lakehouse()
    assert dag.dag_id == "card_transactions_lakehouse"
    assert [t.task_id for t in dag.topological_sort()] == ["ingest", "summarise", "quality_gate"]
    assert dag.max_active_runs == 1
    assert all(t.retries == 2 for t in dag.tasks)
