# VCOM–Yuman Synchronization

This repository provides utilities to synchronise data between the VCOM (meteocontrol) API and Yuman. Synchronisation is broken down into three phases:

1. **Extraction** – gather systems, technical data, inverters and tickets from VCOM using the `VCOMAPIClient` with integrated rate‑limit handling.
2. **Transformation** – map the VCOM payloads to the structure required by Yuman and store mappings in the local `vcom_yuman_mapping.db` database.
3. **Loading** – push the processed information to Yuman and close tickets when needed.

The project currently exposes a production ready VCOM client, helpers for Google Colab environments and a full test suite.

## Requirements

- Python 3.9+
- `requests` (see `requirements.txt`)
- Optional: `google.colab` modules for the smart import solution.

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

Set them manually or create a `.env` file:

```bash
export VCOM_API_KEY="your-key"
export VCOM_USERNAME="your-user"
export VCOM_PASSWORD="your-password"
export YUMAN_TOKEN="your-token"
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
python -m pytest tests/test_vcom.py -v
```
