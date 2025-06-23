dev:
	poetry install

lint:
	poetry run ruff check src tests

test:
	poetry run pytest -q

sync_db:
	poetry run python src/vysync/sync_db_vcom_yuman.py

sync_tickets:
	poetry run python src/vysync/sync_tickets_workorders.py