## Release & Branch Strategy (Best Practice)

Use separate long-lived branches for release channels:

- **`main`** → stable production-ready channel
- **`nightly`** → fast-moving integration channel for newest changes

Recommended workflow:

1. Develop features on short-lived feature branches.
2. Merge feature branches into `nightly` first.
3. Promote tested changes from `nightly` into `main` on cadence.

## Script Layout

To mirror the channels in the repo structure:

- **Stable script:** `stable/BTC3.py`
- **Nightly script:** `nightly/BTC-beta.py`

## Requirements

Install dependencies:

```bash
pip install pandas numpy plotly yfinance requests
```

## Running

Stable channel (`main` branch target):

```bash
python stable/BTC3.py
```

Nightly channel (`nightly` branch target):

```bash
python nightly/BTC-beta.py
```

## Notes

On first run, the scripts will create/use `crypto_data.db` and populate market data.
Subsequent runs are faster due to caching.
