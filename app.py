# -*- coding: utf-8 -*-
import os
import logging
import requests
import json
from flask import Flask, Response, jsonify, current_app
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration (Use Environment Variables on Render) ---
GITHUB_JSON_URL = os.environ.get(
    'GITHUB_JSON_URL',
    'https://raw.githubusercontent.com/byte-capsule/Toffee-Channels-Link-Headers/refs/heads/main/toffee_NS_Player.m3u'
)
DATABASE_API_URL = os.environ.get(
    'DATABASE_API_URL',
    'http://itachi321.pythonanywhere.com' # !!! এটি আপনার ব্যক্তিগত URL, ব্যবহারকারীকে দেখানো হবে না !!!
)

# --- Helper Functions ---

def fetch_from_github():
    """GitHub থেকে JSON ডেটা fetch করে।"""
    logger.info(f"Attempting to fetch data from GitHub source.") # URL লগ করা হচ্ছে না
    try:
        response = requests.get(GITHUB_JSON_URL, timeout=20)
        response.raise_for_status()
        try:
            data = response.json()
            if isinstance(data, list):
                 logger.info(f"Successfully fetched {len(data)} channel entries from GitHub.")
                 return data
            else:
                 logger.error(f"Fetched data from GitHub is not a JSON list. Type: {type(data)}")
                 return None
        except json.JSONDecodeError as json_err:
            logger.error(f"Failed to decode JSON from GitHub URL. Error: {json_err}")
            # debug লগে রেসপন্স রাখা যেতে পারে যদি প্রয়োজন হয়
            # logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Timeout error while fetching data from GitHub.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from GitHub source: {e}") # URL লগ করা হচ্ছে না
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during GitHub fetch: {e}")
        return None

def update_database(channel_data):
    """প্রাপ্ত চ্যানেল ডেটা PythonAnywhere ডাটাবেস সার্ভারে পাঠায়।"""
    if not channel_data:
        logger.warning("No channel data provided to update database.")
        return False

    update_endpoint = f"{DATABASE_API_URL}/api/toffee/update"
    logger.info(f"Attempting to send data to database API endpoint '/api/toffee/update'.") # শুধু পাথ লগ করা হচ্ছে
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(update_endpoint, json=channel_data, headers=headers, timeout=45)
        response.raise_for_status()
        try:
             result = response.json()
             logger.info(f"Database update API response: {result}")
             if response.ok and ("message" in result or result.get("processed_count", 0) > 0 or result.get("added_count", 0) > 0):
                 logger.info("Database update reported success.")
                 return True
             else:
                 logger.warning(f"Database update might have issues based on response: {result}")
                 return False
        except json.JSONDecodeError:
             if response.ok:
                 logger.info(f"Database update successful (Status: {response.status_code}, Non-JSON response).")
                 return True
             else:
                 logger.warning(f"Database update response was not JSON and status was not OK. Status: {response.status_code}, Text: {response.text[:200]}")
                 return False
    except requests.exceptions.Timeout:
        # এরর লগে সম্পূর্ণ URL রাখা যেতে পারে ডিবাগিং এর জন্য
        logger.error(f"Timeout error while sending data to database API: {update_endpoint}")
        return False
    except requests.exceptions.RequestException as e:
        # এরর লগে সম্পূর্ণ URL রাখা যেতে পারে ডিবাগিং এর জন্য
        logger.error(f"Error sending data to database API {update_endpoint}: {e}")
        if e.response is not None:
            logger.error(f"Database API Response Status: {e.response.status_code}")
            logger.error(f"Database API Response Body (first 500 chars): {e.response.text[:500]}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during database update: {e}")
        return False

def sync_data_task(task_name="Scheduled"):
    """GitHub থেকে ডেটা fetch করে ডাটাবেস আপডেট করার scheduled টাস্ক।"""
    logger.info(f"Starting data sync task ({task_name})...")
    with app.app_context():
        channel_data = fetch_from_github()
        if channel_data:
            success = update_database(channel_data)
            if success:
                logger.info(f"Data sync task ({task_name}) completed successfully.")
            else:
                logger.error(f"Data sync task ({task_name}) failed during database update.")
        else:
            logger.error(f"Data sync task ({task_name}) failed: Could not fetch data from GitHub.")

def get_channels_from_db():
    """PythonAnywhere ডাটাবেস সার্ভার থেকে চ্যানেলের তালিকা আনে (JSON list হিসেবে)।"""
    get_endpoint = f"{DATABASE_API_URL}/api/toffee/channels"
    logger.info(f"Attempting to fetch channels from database API endpoint '/api/toffee/channels'.") # শুধু পাথ লগ করা হচ্ছে
    try:
        response = requests.get(get_endpoint, timeout=25)
        response.raise_for_status()
        channels = response.json()
        if isinstance(channels, list):
             logger.info(f"Successfully fetched {len(channels)} channels as JSON list from database API.")
             return channels
        else:
             logger.error(f"Received unexpected data format (not a list) from database API. Type: {type(channels)}")
             return None
    except requests.exceptions.Timeout:
         # এরর লগে সম্পূর্ণ URL রাখা যেতে পারে ডিবাগিং এর জন্য
         logger.error(f"Timeout error while fetching channels from database API: {get_endpoint}")
         return None
    except requests.exceptions.RequestException as e:
        # এরর লগে সম্পূর্ণ URL রাখা যেতে পারে ডিবাগিং এর জন্য
        logger.error(f"Error fetching channels from database API {get_endpoint}: {e}")
        if e.response is not None:
             logger.error(f"DB API Response Status: {e.response.status_code}")
             logger.error(f"DB API Response Body (first 500 chars): {e.response.text[:500]}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"Failed to decode JSON response from database API. Error: {json_err}")
        # logger.debug(f"Response text (first 500 chars): {response.text[:500]}") # প্রয়োজন হলে আনকমেন্ট করুন
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred fetching channels from DB: {e}")
        return None

# --- Flask Application ---
app = Flask(__name__)

# --- Scheduler Setup (অপরিবর্তিত) ---
scheduler = BackgroundScheduler(daemon=True, timezone='Asia/Dhaka')
scheduler.add_job(sync_data_task, 'interval', hours=1, id='hourly_sync_job')
try:
    scheduler.start()
    logger.info("Scheduler started successfully for hourly updates.")
    logger.info("Performing initial data sync on application startup...")
    with app.app_context():
        sync_data_task(task_name="Startup")
    logger.info("Initial data sync task initiated.")
except Exception as e:
    logger.error(f"CRITICAL: Failed to start scheduler or perform initial sync: {e}", exc_info=True)

# --- Flask Routes ---

# --- রুট '/' মুছে ফেলা হয়েছে ---
# @app.route('/')
# def index():
#     ... (এই অংশটি বাদ দেওয়া হয়েছে) ...

@app.route('/health')
def health_check():
    """Health check endpoint for Render."""
    # Render এর জন্য এই রুটটি রাখা ভালো
    return "OK", 200

# --- পরিবর্তিত /toffee.m3u রুট (JSON + Credit) ---
@app.route('/toffee.m3u') # ব্যবহারকারীর অনুরোধ অনুযায়ী এই নামটি রাখা হলো
def serve_toffee_data_as_json():
    """
    ডাটাবেস থেকে Toffee চ্যানেল ডেটা এনে JSON হিসেবে সার্ভ করে, সাথে ক্রেডিট মেসেজ যুক্ত করে।
    """
    logger.info("Request received for /toffee.m3u (serving JSON data with credit)")
    channels = get_channels_from_db() # ডাটাবেস থেকে চ্যানেলের তালিকা পান

    if channels is None:
        # ডেটা আনতে ব্যর্থ হলে JSON ত্রুটি বার্তা দিন
        logger.error("Failed to get channels from DB for JSON response.")
        return jsonify({"error": "Failed to retrieve channel list from the database server."}), 503

    # ক্রেডিট মেসেজ তৈরি করুন (বাংলায়)
    credit_message = "এই তালিকাটি শাহরিয়ার মাহফুজ তৈরি করেছেন, শুধুমাত্র আপনাদের উপভোগের জন্য।"

    # চূড়ান্ত JSON রেসপন্স তৈরি করুন (অবজেক্ট হিসেবে)
    response_data = {
        "credit": credit_message,
        "channels": channels # ডাটাবেস থেকে পাওয়া চ্যানেল লিস্ট
    }

    logger.info(f"Returning {len(channels)} channels as JSON with credit.")
    # jsonify() স্বয়ংক্রিয়ভাবে Content-Type: application/json সেট করে দেয়
    return jsonify(response_data), 200

# --- Run Locally (for testing) ---
if __name__ == '__main__':
    logger.info("Starting JSON Data Server locally for testing...")
    # Check if environment variables are set (optional for local)
    # if not os.environ.get('GITHUB_JSON_URL'):
    #     logger.warning("Environment variable 'GITHUB_JSON_URL' is not set. Using default.")
    # if not os.environ.get('DATABASE_API_URL'):
    #     logger.warning("Environment variable 'DATABASE_API_URL' is not set. Using default.")

    # debug=False রাখুন কারণ Render এটি প্রোডাকশনে চালাবে
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)

                                
