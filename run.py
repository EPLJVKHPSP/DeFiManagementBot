#run.py
import os
import subprocess
import sys

def run_script(script_command):
    """Run a given script command using subprocess."""
    try:
        result = subprocess.run(script_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return "Success"
    except subprocess.CalledProcessError as e:
        return f"Error: {e.stderr.strip()}"

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    messages = []

    # Run pools_data_request.js
    messages.append("Fetching and updating pool data...")
    result = run_script(["node", "pools_data_request.js"])

    # Run strategy_rating.js
    messages.append("\nCalculating strategy ratings...")
    result = run_script(["node", "strategy_rating.js"])

    # Run allocation.py with the current Python interpreter
    messages.append("\nCalculating allocations...")
    result = run_script([sys.executable, "allocation.py"])

    # Join necessary messages into a single string for Telegram
    telegram_message = "\n".join(messages)
    print(telegram_message)

if __name__ == "__main__":
    main()