.PHONY: install land run airflow test clean
install:
	pip install -e ".[dev,airflow]"
land:
	python -m lakehouse.landing --start 2026-10-05 --days 3
run:
	python -m lakehouse.jobs --start 2026-10-05 --days 3
airflow:
	PYTHONPATH=src AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags AIRFLOW__CORE__LOAD_EXAMPLES=False airflow standalone
test:
	pytest -q
clean:
	rm -rf lakehouse_data
