from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackContext, filters
import os
import csv
import subprocess
import logging
from config import BOT_TOKEN
from threading import Thread
from depeg_monitor import load_pairs, monitor_depeg, stop_monitoring
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

allocation_file_path = os.path.join(os.getcwd(), 'allocation.py')  # Ensure correct path
pools_path = os.path.join('data', 'pools.csv')  # Check that 'data' directory exists and is writable

async def start(update: Update, context: CallbackContext) -> None:
    keyboard = [
        [InlineKeyboardButton("🛡️ Depeg Control", callback_data='depeg_control')],
        [InlineKeyboardButton("📊 DeFi Allocation Calculator", callback_data='defi_allocation')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Image file path
    logo_path = 'telegram-pic.jpg'  # Ensure the file is in the correct directory

    # Check if it's from a button press or a command
    if update.message:
        # Send the image first
        await update.message.reply_photo(photo=open(logo_path, 'rb'))  # Sending the image
        # Then send the main menu
        await update.message.reply_text(
            "Management Bot",
            reply_markup=reply_markup
        )
    elif update.callback_query:
        # Send the image first
        await update.callback_query.message.reply_photo(photo=open(logo_path, 'rb'))  # Sending the image
        # Then send the main menu
        await update.callback_query.message.reply_text(
            "Management Bot",
            reply_markup=reply_markup
        )

async def button(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == 'depeg_control':
        keyboard = [
            [InlineKeyboardButton("🔍 Start Control", callback_data='start_depeg')],
            [InlineKeyboardButton("⛔ Stop Control", callback_data='stop_depeg')],
            [InlineKeyboardButton("🔄 Change Pairs", callback_data='change_depeg')],
            [InlineKeyboardButton("⬅️ Main Menu", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text("🛡️ Depeg Control:", reply_markup=reply_markup)

    elif query.data == 'defi_allocation':
        keyboard = [
            [InlineKeyboardButton("⚙️ Start Calculation", callback_data='run_scripts')],
            [InlineKeyboardButton("📋 Show Allocation", callback_data='show_allocation')],
            [InlineKeyboardButton("💰 Change the Amount", callback_data='change_allocation')],
            [InlineKeyboardButton("🔄 Change Pools", callback_data='change_pools')],
            [InlineKeyboardButton("⬅️ Main Menu", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text("📊 DeFi Allocation Calculator:", reply_markup=reply_markup)

    # You can add additional logic for handling back to the main menu
    elif query.data == 'main_menu':
        await start(update, context)  # Redirect to the main menu

    # Other elif blocks remain the same

    elif query.data == 'start_depeg':
        await query.message.reply_text('Depeg monitoring has started.')
        await depeg(update, context)

    elif query.data == 'stop_depeg':
        await query.message.reply_text('Depeg monitoring has stopped.')
        await stop_depeg(update, context)

    elif query.data == 'change_depeg':
        await query.message.reply_text('Please upload a new JSON file to update the depeg tracking configuration.')
        await change_depeg(update, context)

    elif query.data == 'run_scripts':
        await run_scripts(update, context)

    elif query.data == 'show_allocation':
        await show_allocation(update, context)

    elif query.data == 'change_allocation':
        await query.message.reply_text('Please use the command /allocation <amount> to set a new total allocation.')

    elif query.data == 'change_pools':
        await change_pools(update, context)

    elif query.data == 'start_depeg':
        await depeg(update, context)

    elif query.data == 'stop_depeg':
        await stop_depeg(update, context)

    elif query.data == 'change_depeg':
        await change_depeg(update, context)

    elif query.data == 'run_scripts':
        await run_scripts(update, context)

    elif query.data == 'show_allocation':
        await show_allocation(update, context)

    elif query.data == 'change_allocation':
        await update.message.reply_text('Please use the command /allocation <amount> to set a new total allocation.')

    elif query.data == 'change_pools':
        await change_pools(update, context)

    elif query.data == 'main_menu':
        await start(update, context)

async def handle_depeg_control(update: Update, context: CallbackContext, query_data) -> None:
    if query_data == 'start_depeg':
        await depeg(update, context)
    elif query_data == 'stop_depeg':
        await stop_depeg(update, context)
    elif query_data == 'change_depeg':
        await change_depeg(update, context)

async def show_allocation(update: Update, context: CallbackContext) -> None:
    """Send a message with the allocation summary."""
    try:
        with open('allocation_summary.csv', newline='') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Skip the header
            table_data = [f"[{index + 1}] {' | '.join(row)}" for index, row in enumerate(reader)]
        formatted_table = " | ".join(headers) + "\n" + "\n" + "\n".join(table_data)
        
        if update.message:
            await update.message.reply_text(formatted_table)
        elif update.callback_query:
            await update.callback_query.message.reply_text(formatted_table)
    except Exception as e:
        if update.message:
            await update.message.reply_text(f"An error occurred while reading the allocation summary: {str(e)}")
        elif update.callback_query:
            await update.callback_query.message.reply_text(f"An error occurred while reading the allocation summary: {str(e)}")


async def run_scripts(update: Update, context: CallbackContext) -> None:
    if update.message:
        await update.message.reply_text('Running scripts... Please wait.')
    elif update.callback_query:
        await update.callback_query.message.reply_text('Running scripts... Please wait.')

    try:
        # Adding log to verify if the file exists and its last modified time
        pools_path = os.path.join('data', 'pools.csv')
        if os.path.exists(pools_path):
            logging.info(f"{pools_path} last modified at: {os.path.getmtime(pools_path)}")
        
        result = subprocess.run(["python3", "run.py"], capture_output=True, text=True)
        response_message = result.stdout if result.stdout else result.stderr

        if update.message:
            await update.message.reply_text(f"Script executed:\n{response_message}")
        elif update.callback_query:
            await update.callback_query.message.reply_text(f"Script executed:\n{response_message}")
        
        await show_allocation(update, context)
    except Exception as e:
        if update.message:
            await update.message.reply_text(f"An error occurred: {str(e)}")
        elif update.callback_query:
            await update.callback_query.message.reply_text(f"An error occurred: {str(e)}")

async def change_pools(update: Update, context: CallbackContext) -> None:
    """Prompt the user to send a new CSV file to update the pools."""
    if update.message:
        await update.message.reply_text('Please upload a new CSV file to update the pools.')
    elif update.callback_query:
        await update.callback_query.message.reply_text('Please upload a new CSV file to update the pools.')

monitoring_thread = None
monitoring_active = False
monitoring_callback_attached = False  # Add this to manage the callback

# Command to start depeg monitoring
monitoring_thread = None
monitoring_active = False
monitoring_callback_attached = False  # Add this to manage the callback

async def depeg(update: Update, context: CallbackContext) -> None:
    global monitoring_thread, monitoring_active, monitoring_callback_attached

    if monitoring_active:
        if update.callback_query:
            await update.callback_query.message.reply_text('Depeg monitoring is already running.')
        else:
            await update.message.reply_text('Depeg monitoring is already running.')
        return

    monitoring_active = True
    loop = asyncio.get_running_loop()

    if not monitoring_callback_attached:
        # Define async message callback
        async def send_message_async(message: str):
            try:
                if update.callback_query:
                    await update.callback_query.message.reply_text(message)
                else:
                    await update.message.reply_text(message)
                logging.info(f"Sent message: {message}")
            except Exception as e:
                logging.error(f"Error sending message: {str(e)}")

        monitoring_callback_attached = True

    # Start the depeg monitor in a separate thread
    monitoring_thread = Thread(target=lambda: monitor_depeg(send_message_async, loop))
    monitoring_thread.start()

    # Removed reply message here


async def change_depeg(update: Update, context: CallbackContext) -> None:
    """Prompt the user to send a new JSON file to update the depeg tracking configuration."""
    await update.message.reply_text('Please upload a new JSON file to update the depeg tracking configuration.')

# Command to stop depeg monitoring
async def stop_depeg(update: Update, context: CallbackContext) -> None:
    global monitoring_thread, monitoring_active, monitoring_callback_attached

    if not monitoring_active:
        if update.callback_query:
            await update.callback_query.message.reply_text('Depeg monitoring is not currently running.')
        else:
            await update.message.reply_text('Depeg monitoring is not currently running.')
        return

    monitoring_active = False
    monitoring_callback_attached = False

    if monitoring_thread and monitoring_thread.is_alive():
        stop_monitoring()
        monitoring_thread.join()
        monitoring_thread = None

    # Removed reply message here

async def handle_document(update: Update, context: CallbackContext) -> None:
    """Handle document uploads based on file extension."""
    document = update.message.document
    file_extension = document.file_name.split('.')[-1].lower()

    if file_extension == 'csv':
        await process_csv_document(update, context, document)
    elif file_extension == 'json':
        await process_json_document(update, context, document)
    else:
        await update.message.reply_text('Unsupported file type. Please upload a CSV or JSON file.')
        pass

async def process_csv_document(update: Update, context: CallbackContext, document):
    """Process CSV documents."""
    new_file = await context.bot.get_file(document.file_id)
    new_file_path = os.path.join('data', 'pools.csv')
    try:
        # Correct method for downloading files in version 20+
        await new_file.download_to_drive(custom_path=new_file_path)
        # Add your existing CSV processing logic here
        await update.message.reply_text('CSV file updated successfully.')
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {str(e)}")

async def process_json_document(update: Update, context: CallbackContext, document):
    """Process JSON documents."""
    new_file = await context.bot.get_file(document.file_id)
    new_file_path = os.path.join(os.getcwd(), 'depeg_tracking.json')
    try:
        # Correct method for downloading files in version 20+
        await new_file.download_to_drive(custom_path=new_file_path)

        # After downloading, reload the pairs and reset the price histories
        pairs = load_pairs()
        logging.info(f"New pairs loaded from updated JSON: {pairs}")
        await update.message.reply_text('JSON file updated successfully, and pairs have been reloaded.')
    except Exception as e:
        logging.error(f"Error processing JSON document: {str(e)}")
        await update.message.reply_text(f"An error occurred while processing the JSON file: {str(e)}")

async def set_allocation(update: Update, context: CallbackContext) -> None:
    """Set a new total allocation in allocation.py."""
    try:
        new_allocation = int(context.args[0])
        allocation_file_path = 'allocation.py'

        # Ensure the file path is correct and accessible
        if not os.path.exists(allocation_file_path):
            logging.error(f"{allocation_file_path} does not exist.")
            await update.message.reply_text('Allocation file does not exist.')
            return

        # Read the existing allocation.py file
        with open(allocation_file_path, 'r') as file:
            lines = file.readlines()

        # Modify the line containing the allocation variable
        updated = False
        for i, line in enumerate(lines):
            if 'allocation =' in line:
                lines[i] = f'allocation = {new_allocation}\n'
                updated = True
                logging.info(f"Allocation updated to {new_allocation}")
                break

        if not updated:
            logging.error('No allocation line found in the file.')
            await update.message.reply_text('No allocation line found in the file.')
            return

        # Write the changes back to allocation.py
        with open(allocation_file_path, 'w') as file:
            file.writelines(lines)

        await update.message.reply_text(f'Total allocation set to {new_allocation}.')
    except IndexError:
        await update.message.reply_text('Please provide a valid number for the allocation. Usage: /allocation <amount>')
    except ValueError:
        await update.message.reply_text('Invalid number provided.')
    except Exception as e:
        logging.error(f'Error setting allocation: {str(e)}')
        await update.message.reply_text(f"An error occurred while setting the allocation: {str(e)}")
        pass

def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()

    # Handlers for Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("run", run_scripts))
    app.add_handler(CommandHandler("show_allocation", show_allocation))
    app.add_handler(CommandHandler("change_pools", change_pools))
    app.add_handler(CommandHandler("allocation", set_allocation))
    app.add_handler(CommandHandler("depeg", depeg))
    app.add_handler(CommandHandler("stop_depeg", stop_depeg))
    app.add_handler(CommandHandler("change_depeg", change_depeg))
    
    # Document Handlers
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Button handler
    app.add_handler(CallbackQueryHandler(button))

    # Start the bot
    app.run_polling()

if __name__ == '__main__':
    main()