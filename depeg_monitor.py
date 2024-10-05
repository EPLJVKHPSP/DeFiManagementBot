import time
import json
import logging
import asyncio
import requests
from telegram.ext import Application

# Standart logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dexscreener API
# Here we get info about DEX Pair each 10 sec and storing 100 last updates
pairs_config_path = 'depeg_tracking.json'
API_URL = "https://api.dexscreener.com/latest/dex/search"
MAX_HISTORY_SIZE = 100
SLEEP_INTERVAL = 10

# Global variable to control the monitoring loop
monitoring_active = True

def load_pairs():
    global price_histories
    try:
        with open(pairs_config_path, 'r') as file:
            data = json.load(file)
            new_pairs = data['pairs']

            # Reset the price history for the new pairs
            price_histories = {pair: [] for pair in new_pairs}
            logging.info(f"Updated pairs and reset price histories: {new_pairs}")
            return new_pairs
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {pairs_config_path}")
        exit(1)
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from the file: {pairs_config_path}")
        exit(1)


# List where we're storing history of DEX pairs
price_histories = {pair: [] for pair in load_pairs()}

def fetch_current_price(pair):
    try:
        response = requests.get(f"{API_URL}?q={pair}")
        response.raise_for_status()
        data = response.json()
        if 'pairs' in data and data['pairs']:
            return float(data['pairs'][0]['priceNative'])
    except requests.RequestException as e:
        logging.error(f"Request error for {pair}: {e}")
    except ValueError as e:
        logging.error(f"Value error for {pair}: {e}")
    return None

def update_price_history(pair, price):
    history = price_histories[pair]
    if len(history) >= MAX_HISTORY_SIZE:
        history.pop(0)
    history.append(price)


# Calcualting average for the pair form the price_histories
def calculate_average(history):
    return sum(history) / len(history) if history else 0

def calculate_deviation(current_price, average_price):
    return (current_price - average_price) / average_price if average_price != 0 else 0

def monitor_depeg(bot_message_callback, loop):
    global monitoring_active
    monitoring_active = True  # Set to True to ensure it starts correctly.
    
    while monitoring_active:
        pairs = load_pairs()  # Ensure the latest pairs are used

        for pair in pairs:
            logging.info(f"Checking price for {pair}.")
            try:
                current_price = fetch_current_price(pair)

                if current_price is not None:
                    logging.info(f"Price for {pair}: {current_price}")
                    update_price_history(pair, current_price)

                    history = price_histories[pair]
                    average_price = calculate_average(history)

                    deviation = calculate_deviation(current_price, average_price)
                    logging.info(f"{pair}: Current Price = {current_price}, Average = {average_price}, Deviation = {deviation:.2%}")
                    
                    if deviation <= -0.01:  # Deviation of -1% considers as a depeg of an asset
                        
                        message = f"Depeg detected! {pair} has deviated by {deviation:.2%} from the average."
                        logging.info(f"Notification triggered for {pair}: {message}")
                        asyncio.run_coroutine_threadsafe(bot_message_callback(message), loop)
                else:
                    logging.info(f"Failed to fetch current price for {pair}.")
            except Exception as e:
                logging.error(f"Error processing data for {pair}: {str(e)}")
        time.sleep(SLEEP_INTERVAL)
    logging.info("Monitoring loop ended.")

def stop_monitoring():
    global monitoring_active
    monitoring_active = False