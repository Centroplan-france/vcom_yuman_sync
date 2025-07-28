dev:
	poetry install

lint:
	poetry run ruff check src tests

test:
	poetry run pytest -q

sync_db:
	poetry run python -m vysync.cli

sync_tickets:
	poetry run python -m vysync.sync_tickets_workorders