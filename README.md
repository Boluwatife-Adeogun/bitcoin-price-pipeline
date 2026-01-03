# Bitcoin price pipeline 🚀
A  Python ETL pipeline that fetches live Bitcoin prices from the CoinGecko API every 5 minutes, validates the data, logs issues, cleans it with timestamps, and stores it in SQLite, complete with unit tests.

---------------------------------

## 📊 Sample Output
After running the pipeline for a while, your bitcoin_prices.db will contain rows like this (view with any SQLite browser):

| timestamp                  | price_usd   |
|----------------------------|-------------|
| 2026-01-03T10:15:23.123456 | 88456.78   |
| 2026-01-03T10:20:23.456789 | 88512.34   |
| ...                        | ...        |

Logs (pipeline.log) show successes and any errors

-----------------------------------------------------

## Project Overview
This project demonstrates essential data engineering best practices:
- Robust API fetching with timeout and error handling
- Strict data validation (prevents crashes if API format changes)
- Comprehensive logging to file and console
- Automated scheduling (runs indefinitely every 5 minutes)
- Clean, modular code with clear functions
- Unit tests for validation and cleaning logic

 ------------------------------------------
## 🛠 Tech Stack

- *Python 3.8+*
- requests – API calls
- schedule – Timing the runs
- sqlite3 (built-in) – Lightweight database
- logging (built-in) – Error/success tracking
- pytest – Unit testing

## 🚀 Quick Start

### 1. Prerequisites
- Python 3.8 or higher installed
- Git (optional, for cloning)

### 2. Clone and Setup
```bash
git clone https://github.com/Boluwatife-Adeogun/bitcoin-price-pipeline

## install depedencies
in requirements.txt

-----------------------------------------
## Project Structure
bitcoin-price-pipeline/
├── bitcoin_price_pipeline.py   # Main ETL script
├── test_pipeline.py            # Unit tests
├── pipeline.log                # Auto-generated logs
├── bitcoin_prices.db           # Auto-generated database
├── .gitignore                  # Ignores venv, logs, db
└── README.md                   # This file


------------------------------------------------------------

## Author
Adeogun Boluwatife - Data Scientist
linkdln: https://www.linkedin.com/in/boluwatife-a

 



