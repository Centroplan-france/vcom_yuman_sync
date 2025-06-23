# VCOM–Yuman Synchronization

This repository provides utilities to synchronise data between the VCOM (meteocontrol) API and Yuman. Synchronisation is broken down into three phases:

1. **Extraction** – gather systems, technical data, inverters and tickets from VCOM using the `VCOMAPIClient` with integrated rate‑limit handling.
2. **Transformation** – map the VCOM payloads to the structure required by Yuman and store mappings in the local `vcom_yuman_mapping.db` database.
3. **Loading** – push the processed information to Yuman and close tickets when needed.

Additional conflicts detected during sync are stored in the `conflicts` table for later review.

The project currently exposes a production ready VCOM client, a full test suite.

## Requirements

This project targets **Python 3.11** and uses [Poetry](https://python-poetry.org/) to
manage dependencies. A `requirements.txt` file is also provided for `pip`
installations.

Install everything with Poetry:

```bash
poetry install
```

Or using `pip`:

```bash
pip install -r requirements.txt
```

## Environment Variables

These credentials must be available in your environment:

- `VCOM_API_KEY` – VCOM API key
- `VCOM_USERNAME` – VCOM account username
- `VCOM_PASSWORD` – VCOM account password
- `YUMAN_TOKEN` – API token for Yuman
- `DATABASE_URL` – connection string for the local Postgres/Supabase database
- `SUPABASE_URL` – optional Supabase project URL (for REST upserts)
- `SUPABASE_SERVICE_KEY` – service key associated with the project
- `LOG_LEVEL` – optional log verbosity (default `INFO`)

You can export them manually or place them inside a local `.env` file. The
library `python-dotenv` is installed with the project and all modules call
`load_dotenv()` so the variables are picked up automatically. Use the
`.env.example` file as a template and create your own `.env` file at the
repository root:

```bash
cp .env.example .env
```

Edit `.env` and provide your real credentials:

```dotenv
VCOM_API_KEY=your-key
VCOM_USERNAME=your-user
VCOM_PASSWORD=your-password
YUMAN_TOKEN=your-token
DATABASE_URL=postgresql://user:password@localhost/dbname
SUPABASE_URL=https://xyzcompany.supabase.co
SUPABASE_SERVICE_KEY=your-service-key
```

## Basic Usage

```python
from vcom_client import VCOMAPIClient

client = VCOMAPIClient()
systems = client.get_systems()
print(f"{len(systems)} systems found")
```

## CLI Utilities

Several helper scripts are included:

- `scripts/supabase_setup.py` – initialise the database schema and import Yuman
  equipment categories.
- `vysync/sync_db_vcom_yuman.py` – synchronise sites and equipment between VCOM,
  Yuman and the local database.
- `vysync/sync_tickets_workorders.py` – keep VCOM tickets and Yuman work orders
  in sync.

All scripts load environment variables automatically, so ensure your `.env`
file is correctly configured before running them.

## Tests

Compile the modules and run the tests using pytest:

```bash
python -m py_compile $(git ls-files '*.py')
python -m pytest -v
```

## GitHub Actions Secrets

When running this project in GitHub Actions, store the credentials as
repository secrets so they are injected as environment variables. Add the
following secrets in your repository settings:

- `VCOM_API_KEY`
- `VCOM_USERNAME`
- `VCOM_PASSWORD`
- `YUMAN_TOKEN`

These secrets can then be referenced in a workflow like:

```yaml
env:
  VCOM_API_KEY: ${{ secrets.VCOM_API_KEY }}
  VCOM_USERNAME: ${{ secrets.VCOM_USERNAME }}
  VCOM_PASSWORD: ${{ secrets.VCOM_PASSWORD }}
  YUMAN_TOKEN: ${{ secrets.YUMAN_TOKEN }}
```
