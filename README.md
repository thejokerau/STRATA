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
pip install pandas numpy plotly yfinance requests pandas_ta optuna
```

Python:

- Nightly script targets **Python 3.13+** and is primarily tested on **3.13.x**.

Optional acceleration dependency:

```bash
pip install cupy-cuda12x
```

If CuPy/CUDA is not available, nightly falls back to CPU automatically.

## Running

Stable channel (`main` branch target):

```bash
python stable/BTC3.py
```

Nightly channel (`nightly` branch target):

```bash
python nightly/BTC-beta.py
```

## Nightly Highlights (`nightly/BTC-beta.py`)

- User-selectable analysis interval: `1d`, `4h`, `8h`, `12h`
- Backtest lookback selector: `1/3/6/12/18/24 months`
- Accurate period math using `365.25` days/year
- Dynamic Fibonacci swing detection with support/resistance guards
- ATR/ADX/OBV/CMF risk-scoring integration
- Walk-forward auto-tuning with Optuna
- CPU parallelism:
  - Indicator cache build worker selection
  - Optuna trial parallel jobs
- CUDA detection via CuPy with safe CPU fallback
- Binance symbol pre-filtering using `exchangeInfo` to reduce bad ticker noise
- Backtest churn controls:
  - `min_hold_bars` before signal exits
  - `cooldown_bars` after exits

## Notes

On first run, the scripts will create/use `crypto_data.db` and populate market data.
Subsequent runs are faster due to caching.
