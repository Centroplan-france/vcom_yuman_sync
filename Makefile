dev:
	poetry install

lint:
	poetry run ruff check tests

test:
	poetry run pytest -q
