import requests
import logging
import sqlite3
import datetime
import schedule
import time

# -------------------- Logging setup
logging.basicConfig(

    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

#---------------------Database setup
#create/connect to SQLite database
conn = sqlite3.connect('bitcoin_prices.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS prices (
        timestamp TEXT PRIMARY KEY,
        price_usd REAL
    )
''')
conn.commit()

#-----------------------functions

def fetch_bitcoin_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises error if not 200 OK
        data = response.json()
        logging.info("Successfully fetched data from API")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None


def validate_data(data):
    """Validate the structure and types of the API response"""
    if data is None:
        return False, "No data received"
    
    if not isinstance(data, dict):
        return False, "Data is not a dictionary"
    
    if "bitcoin" not in data:
        return False, "Missing 'bitcoin' key"
    
    bitcoin_data = data["bitcoin"]
    if not isinstance(bitcoin_data, dict):
        return False, "'bitcoin' value is not a dictionary"
    
    if "usd" not in bitcoin_data:
        return False, "Missing 'usd' key"
    
    if not isinstance(bitcoin_data["usd"], (int, float)):
        return False, "'usd' value is not a number"
    
    return True, "Data is valid"


def clean_data(data):
    """Extract price and add timestamp"""
    price = data["bitcoin"]["usd"]
    timestamp = datetime.datetime.now().isoformat()
    return {"timestamp": timestamp, "price_usd": price}

def save_to_db(cleaned_data):
    """Save cleaned data to SQLite database"""
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO prices (timestamp, price_usd)
            VALUES (?, ?)
        ''', (cleaned_data["timestamp"], cleaned_data["price_usd"]))
        conn.commit()
        logging.info(f"Data saved: {cleaned_data['price_usd']} USD at {cleaned_data['timestamp']}")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")

def run_pipeline():
    """Ful ETL: fetch,validate,clean,save"""
    raw_data = fetch_bitcoin_price()
    is_valid, message = validate_data(raw_data)
    if not is_valid:
        logging.error(f"Validation failed: {message}")
        return  # Stop if invalid
    
    cleaned_data = clean_data(raw_data)
    save_to_db(cleaned_data)

if __name__ == "__main__":
    logging.info("Starting Bitcoin price pipeline - running every 5 minutes")
    schedule.every(5).minutes.do(run_pipeline)
    
    # Run once immediately on start (optional but helpful)
    run_pipeline()
    
    # Keep the script running
    while True:
       schedule.run_pending()
       time.sleep(1)
