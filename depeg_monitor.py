import time
import logging
import asyncio
import requests
from telegram.ext import Application
from db_util import get_connection  # Import your database connection utility

# Standard logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dexscreener API
API_URL = "https://api.dexscreener.com/latest/dex/search"
MAX_HISTORY_SIZE = 100
SLEEP_INTERVAL = 10

# Global variable to control the monitoring loop
monitoring_active = True
price_histories = {}  # Dictionary to hold price history for each pair


# Fetch pairs from PostgreSQL database
def fetch_pairs_from_db():
    """
    Fetches the list of token pairs to monitor from the 'ratings.depeg' table in PostgreSQL.
    Returns:
        List of token pairs.
    """
    try:
        conn = get_connection()  # Connect to the database
        with conn.cursor() as cursor:
            cursor.execute("SELECT token FROM ratings.depeg;")
            pairs = [row[0] for row in cursor.fetchall()]
            logging.info(f"Fetched pairs from database: {pairs}")
            return pairs
    except Exception as e:
        logging.error(f"Error fetching pairs from database: {e}")
        return []
    finally:
        if conn:
            conn.close()


def fetch_current_price(pair):
    """
    Fetches the current price for a given token pair from the Dexscreener API.
    Args:
        pair (str): The token pair to fetch.
    Returns:
        float: Current price if successful, None otherwise.
    """
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
    """
    Updates the price history for a given pair, maintaining a maximum size.
    Args:
        pair (str): The token pair.
        price (float): The current price to append.
    """
    if pair not in price_histories:
        price_histories[pair] = []
    history = price_histories[pair]
    if len(history) >= MAX_HISTORY_SIZE:
        history.pop(0)
    history.append(price)


def calculate_average(history):
    """
    Calculates the average price from the price history.
    Args:
        history (list): List of historical prices.
    Returns:
        float: Average price.
    """
    return sum(history) / len(history) if history else 0


def calculate_deviation(current_price, average_price):
    """
    Calculates the deviation between the current price and the average price.
    Args:
        current_price (float): The current price.
        average_price (float): The average price.
    Returns:
        float: Deviation percentage.
    """
    return (current_price - average_price) / average_price if average_price != 0 else 0


def monitor_depeg(bot_message_callback, loop):
    """
    Main monitoring loop to track price deviations for token pairs.
    Args:
        bot_message_callback (function): Callback to send messages.
        loop (asyncio.AbstractEventLoop): Event loop for sending messages.
    """
    global monitoring_active
    monitoring_active = True  # Set to True to ensure it starts correctly.

    while monitoring_active:
        pairs = fetch_pairs_from_db()  # Fetch pairs dynamically from the database

        if not pairs:
            logging.warning("No token pairs found in the database. Retrying...")
            time.sleep(SLEEP_INTERVAL)
            continue

        for pair in pairs:
            logging.info(f"Checking price for {pair}.")
            try:
                current_price = fetch_current_price(pair)

                if current_price is not None:
                    logging.info(f"Price for {pair}: {current_price}")
                    update_price_history(pair, current_price)

                    history = price_histories.get(pair, [])
                    average_price = calculate_average(history)

                    deviation = calculate_deviation(current_price, average_price)
                    logging.info(f"{pair}: Current Price = {current_price}, Average = {average_price}, Deviation = {deviation:.2%}")

                    if deviation <= -0.01:  # Deviation of -1% triggers a depeg notification
                        message = f"⚠️ Depeg detected! {pair} has deviated by {deviation:.2%} from the average."
                        logging.info(f"Notification triggered for {pair}: {message}")
                        asyncio.run_coroutine_threadsafe(bot_message_callback(message), loop)
                else:
                    logging.info(f"Failed to fetch current price for {pair}.")
            except Exception as e:
                logging.error(f"Error processing data for {pair}: {str(e)}")
        time.sleep(SLEEP_INTERVAL)
    logging.info("Monitoring loop ended.")


def stop_monitoring():
    """
    Stops the monitoring loop.
    """
    global monitoring_active
    monitoring_active = False
    logging.info("Monitoring stopped.")