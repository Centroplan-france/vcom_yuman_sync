dev:
	poetry install

lint:
	poetry run ruff check src tests

test:
	poetry run pytest -q
