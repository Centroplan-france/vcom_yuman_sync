# VCOM-Yuman Sync

This project provides utilities for interacting with the VCOM and Yuman APIs.

## Required Environment Variables

The modules expect the following variables to be present in the environment:

- `VCOM_API_KEY`
- `VCOM_USERNAME`
- `VCOM_PASSWORD`
- `YUMAN_TOKEN`

These credentials are read at runtime and are required for the clients to
authenticate with the corresponding services.

## Installing Dependencies

Install Python dependencies using `pip` and the provided `requirements.txt`:

```bash
pip install -r requirements.txt
```

Python 3.7 or later is required.

## Running Checks and Tests

Before running the tests you can verify that all modules compile:

```bash
python -m py_compile $(git ls-files '*.py')
```

Execute the unit tests with `pytest`:

```bash
pytest -q
```
