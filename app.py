# -*- coding: utf-8 -*-
import os
import logging
import requests
import json
from flask import Flask, Response, jsonify, current_app
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time # Delay এর জন্য time import করা যেতে পারে

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration (Use Environment Variables on Render) ---
# আপনার GitHub থেকে Toffee চ্যানেলের JSON ডেটা যেখানে আছে সেই RAW লিংক
GITHUB_JSON_URL = os.environ.get(
    'GITHUB_JSON_URL',
    'https://raw.githubusercontent.com/byte-capsule/Toffee-Channels-Link-Headers/refs/heads/main/toffee_NS_Player.m3u'
)
# আপনার PythonAnywhere ডাটাবেস সার্ভারের API বেস URL (আপনার ইউজারনেম দিয়ে ಬದಲಾಯಿಸಿ)
DATABASE_API_URL = os.environ.get(
    'DATABASE_API_URL',
    'http://itachi321.pythonanywhere.com' # !!! নিশ্চিত করুন এটি আপনার সঠিক URL !!!
)

# --- Helper Functions ---

def fetch_from_github():
    """GitHub থেকে JSON ডেটা fetch করে।"""
    logger.info(f"Attempting to fetch data from GitHub: {GITHUB_JSON_URL}")
    try:
        response = requests.get(GITHUB_JSON_URL, timeout=20) # টাইমআউট সামান্য বাড়ানো হলো
        response.raise_for_status() # HTTP ত্রুটির জন্য exception raise করবে (e.g., 404, 500)
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
            logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"Timeout error while fetching data from GitHub: {GITHUB_JSON_URL}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from GitHub URL {GITHUB_JSON_URL}: {e}")
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
    logger.info(f"Attempting to send data to database API: {update_endpoint}")
    try:
        headers = {'Content-Type': 'application/json'}
        # ডেটাবেস সার্ভার রেডি হতে সময় লাগতে পারে, তাই এখানেও টাইমআউট যথেষ্ট রাখা ভালো
        response = requests.post(update_endpoint, json=channel_data, headers=headers, timeout=45)
        response.raise_for_status() # HTTP ত্রুটির জন্য exception raise করবে

        try:
             result = response.json()
             logger.info(f"Database update API response: {result}")
             # Success criteria: Check for message or positive counts
             if response.ok and ("message" in result or result.get("processed_count", 0) > 0 or result.get("added_count", 0) > 0):
                 logger.info("Database update reported success.")
                 return True
             else:
                 logger.warning(f"Database update might have issues based on response: {result}")
                 return False # Assume failure if response structure isn't as expected
        except json.JSONDecodeError:
             # Handle non-JSON success response (e.g., 200 OK with simple text)
             if response.ok:
                 logger.info(f"Database update successful (Status: {response.status_code}, Non-JSON response).")
                 return True
             else:
                 logger.warning(f"Database update response was not JSON and status was not OK. Status: {response.status_code}, Text: {response.text[:200]}")
                 return False

    except requests.exceptions.Timeout:
        logger.error(f"Timeout error while sending data to database API: {update_endpoint}")
        return False
    except requests.exceptions.RequestException as e:
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
    # Flask অ্যাপ কনটেক্সট এর মধ্যে রান করা ভালো যদি current_app বা g ব্যবহার করা হয়
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
    """PythonAnywhere ডাটাবেস সার্ভার থেকে চ্যানেলের তালিকা আনে।"""
    get_endpoint = f"{DATABASE_API_URL}/api/toffee/channels"
    logger.info(f"Attempting to fetch channels from database API: {get_endpoint}")
    try:
        response = requests.get(get_endpoint, timeout=25) # টাইমআউট সামঞ্জস্য করা হলো
        response.raise_for_status()
        channels = response.json()
        if isinstance(channels, list):
             logger.info(f"Successfully fetched {len(channels)} channels from database API.")
             return channels
        else:
             logger.error(f"Received non-list data from database API (Type: {type(channels)}).")
             return None

    except requests.exceptions.Timeout:
         logger.error(f"Timeout error while fetching channels from database API: {get_endpoint}")
         return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching channels from database API {get_endpoint}: {e}")
        if e.response is not None:
             logger.error(f"DB API Response Status: {e.response.status_code}")
             logger.error(f"DB API Response Body (first 500 chars): {e.response.text[:500]}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"Failed to decode JSON response from database API. Error: {json_err}")
        logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred fetching channels from DB: {e}")
        return None

def generate_m3u(channels):
    """চ্যানেল লিস্ট থেকে M3U প্লেলিস্ট স্ট্রিং তৈরি করে।"""
    if channels is None: # Check specifically for None, as empty list is valid
         logger.error("Cannot generate M3U: Failed to retrieve channels from DB.")
         return "#EXTM3U\n#EXTINF:-1,Error\n# Error: Could not retrieve channel list from the database server."
    if not channels:
        logger.warning("Generating M3U for an empty channel list.")
        return "#EXTM3U\n#EXTINF:-1,No Channels Found\n# Channel list is currently empty."

    m3u_content = ["#EXTM3U"]
    group_title = "Toffee" # Static group title

    processed_count = 0
    for channel in channels:
        name = channel.get('name', '').strip()
        logo = channel.get('logo', '').strip()
        link = channel.get('link', '').strip()

        # Ensure required fields are present and not empty
        if not name or not link:
            logger.warning(f"Skipping channel due to missing name ('{name}') or link ('{link}'). Logo: '{logo}'")
            continue

        # M3U ট্যাগ ফরম্যাট করুন
        extinf_line = f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}" tvg-logo="{logo}" group-title="{group_title}",{name}'
        m3u_content.append(extinf_line)
        m3u_content.append(link)
        processed_count += 1

    logger.info(f"Generated M3U playlist with {processed_count} valid channel entries.")
    return "\n".join(m3u_content)

# --- Flask Application ---
app = Flask(__name__)

# --- Scheduler Setup ---
# daemon=True হলে অ্যাপ এক্সিট করলে শিডিউলার বন্ধ হবে
# timezone আপনার এলাকার জন্য সেট করুন
scheduler = BackgroundScheduler(daemon=True, timezone='Asia/Dhaka')

# টাস্কটি প্রতি ঘণ্টা অন্তর চলবে। প্রথম রানটি নিচের কোড দ্বারা ম্যানুয়ালি ট্রিগার করা হবে।
# ስለዚህ প্রথম schedule হওয়া রানটি ১ ঘণ্টা পর হবে।
scheduler.add_job(sync_data_task, 'interval', hours=1, id='hourly_sync_job')

try:
    scheduler.start()
    logger.info("Scheduler started successfully for hourly updates.")

    # --- Perform initial data sync on startup ---
    logger.info("Performing initial data sync on application startup...")
    # অ্যাপ কনটেক্সট এর মধ্যে টাস্কটি রান করুন
    # এটি non-blocking ভাবে রান করানো যেতে পারে যদি স্টার্টআপ টাইম বেশি হয়,
    # তবে সহজ রাখার জন্য সরাসরি কল করা হলো।
    with app.app_context():
        # sync_data_task ফাংশনে একটি আর্গুমেন্ট যোগ করে এটিকে 'Startup' টাস্ক হিসেবে চিহ্নিত করুন
        sync_data_task(task_name="Startup")
    logger.info("Initial data sync task initiated.")
    # --- End of initial sync ---

except Exception as e:
    # যদি শিডিউলার স্টার্ট হতে বা ইনিশিয়াল সিঙ্ক হতে ফেইল করে
    logger.error(f"CRITICAL: Failed to start scheduler or perform initial sync: {e}", exc_info=True)
    # এখানে আপনি একটি নোটিফিকেশন পাঠাতে পারেন বা অন্য ব্যবস্থা নিতে পারেন

# --- Flask Routes ---

@app.route('/')
def index():
    """সার্ভার চলছে কিনা তা জানার জন্য একটি সাধারণ রুট।"""
    next_run = None
    try:
        job = scheduler.get_job('hourly_sync_job')
        if job:
            next_run = job.next_run_time.isoformat() if job.next_run_time else "Not scheduled yet"
    except Exception as e:
        logger.warning(f"Could not get scheduler next run time: {e}")
        next_run = "Error fetching schedule"

    return jsonify({
        "status": "M3U Playlist Server is running",
        "playlist_url": "/toffee.m3u",
        "github_source": GITHUB_JSON_URL,
        "database_api": DATABASE_API_URL,
        "scheduler_status": "Running" if scheduler.running else "Stopped",
        "next_scheduled_sync": next_run
        })

@app.route('/health')
def health_check():
    """Health check endpoint for Render."""
    # এখানে ডাটাবেস বা অন্য সার্ভিসের কানেক্টিভিটি চেক করা যেতে পারে
    # আপাতত শুধু OK রিটার্ন করা হচ্ছে
    return "OK", 200

@app.route('/toffee.m3u')
def serve_toffee_m3u():
    """ডাটাবেস থেকে ডেটা এনে M3U প্লেলিস্ট সার্ভ করে।"""
    logger.info("Request received for /toffee.m3u")
    channels = get_channels_from_db() # ডাটাবেস থেকে চ্যানেলের তালিকা পান

    # generate_m3u ফাংশন None এবং খালি লিস্ট হ্যান্ডেল করবে
    m3u_playlist = generate_m3u(channels)

    # যদি 채널 আনতে ব্যর্থ হয়, generate_m3u একটি error message সহ স্ট্রিং দেবে
    status_code = 200
    if channels is None:
        logger.error("Failed to get channels from DB for M3U generation.")
        status_code = 503 # Service Unavailable

    # সঠিক MIME টাইপ এবং স্ট্যাটাস কোড সহ রেসপন্স দিন
    return Response(m3u_playlist, mimetype='application/vnd.apple.mpegurl', status=status_code)

# --- Application Cleanup (Optional) ---
# অ্যাপ বন্ধ করার সময় শিডিউলার সুন্দরভাবে বন্ধ করা
# def shutdown_hook():
#     logger.info("Shutting down scheduler...")
#     scheduler.shutdown()
# import atexit
# atexit.register(shutdown_hook)

# --- Run Locally (for testing) ---
# Render gunicorn ব্যবহার করবে, তাই এই অংশটি সেখানে সরাসরি রান হবে না
if __name__ == '__main__':
    logger.info("Starting M3U Playlist Server locally for testing...")
    # Check if environment variables are set
    if not os.environ.get('GITHUB_JSON_URL'):
        logger.warning("Environment variable 'GITHUB_JSON_URL' is not set. Using default.")
    if not os.environ.get('DATABASE_API_URL'):
        logger.warning("Environment variable 'DATABASE_API_URL' is not set. Using default.")

    # DO NOT use debug=True in production on Render
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)

