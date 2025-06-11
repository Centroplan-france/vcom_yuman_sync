# VCOM–Yuman Synchronization

This repository provides utilities to synchronise data between the VCOM (meteocontrol) API and Yuman. Synchronisation is broken down into three phases:

1. **Extraction** – gather systems, technical data, inverters and tickets from VCOM using the `VCOMAPIClient` with integrated rate‑limit handling.
2. **Transformation** – map the VCOM payloads to the structure required by Yuman and store mappings in the local `vcom_yuman_mapping.db` database.
3. **Loading** – push the processed information to Yuman and close tickets when needed.

Additional conflicts detected during sync are stored in the `conflicts` table for later review.

The project currently exposes a production ready VCOM client, a full test suite.

## Requirements

- Python 3.9+
- `requests` (see `requirements.txt`)
- `python-dotenv` for loading environment variables from a `.env` file

Install the dependencies with:

```bash
pip install -r requirements.txt
```

## Environment Variables

These credentials must be available in your environment:

- `VCOM_API_KEY` – VCOM API key
- `VCOM_USERNAME` – VCOM account username
- `VCOM_PASSWORD` – VCOM account password
- `YUMAN_TOKEN` – API token for Yuman
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
```

## Basic Usage

```python
from vcom_client import VCOMAPIClient

client = VCOMAPIClient()
systems = client.get_systems()
print(f"{len(systems)} systems found")
```

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
