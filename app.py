import os
import logging
import requests
import json
from flask import Flask, Response, jsonify, current_app
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

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
    'http://itachi321.pythonanywhere.com' # নিশ্চিত করুন এটি আপনার সঠিক URL
)

# --- Helper Functions ---

def fetch_from_github():
    """GitHub থেকে JSON ডেটা fetch করে।"""
    try:
        response = requests.get(GITHUB_JSON_URL, timeout=15) # 15 সেকেন্ড টাইমআউট
        response.raise_for_status() # HTTP ত্রুটির জন্য exception raise করবে (e.g., 404, 500)
        # সরাসরি JSON ডেটা পার্স করার চেষ্টা
        try:
            data = response.json()
            if isinstance(data, list):
                 logger.info(f"Successfully fetched {len(data)} channel entries from GitHub.")
                 return data
            else:
                 logger.error("Fetched data from GitHub is not a JSON list.")
                 return None
        except json.JSONDecodeError as json_err:
            logger.error(f"Failed to decode JSON from GitHub URL. Error: {json_err}")
            logger.debug(f"Response text was: {response.text[:500]}") # প্রথম ৫০০ অক্ষর লগ করুন
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
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(update_endpoint, json=channel_data, headers=headers, timeout=30) # 30 সেকেন্ড টাইমআউট
        response.raise_for_status() # HTTP ত্রুটির জন্য exception raise করবে

        # রেসপন্স JSON হলে সেটি লগ করুন
        try:
             result = response.json()
             logger.info(f"Database update response: {result}")
             # এখানে আপনি রেসপন্স এর উপর ভিত্তি করে আরও কিছু করতে পারেন (e.g., added_count চেক করা)
             if result.get("added_count", 0) > 0 or result.get("message"):
                 logger.info("Database update successful.")
                 return True
             else:
                 logger.warning(f"Database update might have issues, response: {result}")
                 return False
        except json.JSONDecodeError:
             logger.warning(f"Database update response was not JSON. Status: {response.status_code}, Text: {response.text[:200]}")
             # স্ট্যাটাস 2xx হলে 성공 ধরে নেওয়া যায়
             return response.ok

    except requests.exceptions.Timeout:
        logger.error(f"Timeout error while sending data to database API: {update_endpoint}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending data to database API {update_endpoint}: {e}")
        # রেসপন্স থাকলে লগ করুন
        if e.response is not None:
            logger.error(f"Database API Response Status: {e.response.status_code}")
            logger.error(f"Database API Response Body: {e.response.text[:500]}") # পুরো বডি লগ না করাই ভালো
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during database update: {e}")
        return False

def sync_data_task():
    """GitHub থেকে ডেটা fetch করে ডাটাবেস আপডেট করার scheduled টাস্ক।"""
    logger.info("Starting scheduled data sync task...")
    with app.app_context(): # Flask অ্যাপ কনটেক্সট এর মধ্যে রান করা ভালো যদি current_app ব্যবহার করেন
        channel_data = fetch_from_github()
        if channel_data:
            success = update_database(channel_data)
            if success:
                logger.info("Data sync task completed successfully.")
            else:
                logger.error("Data sync task failed during database update.")
        else:
            logger.error("Data sync task failed: Could not fetch data from GitHub.")

def get_channels_from_db():
    """PythonAnywhere ডাটাবেস সার্ভার থেকে চ্যানেলের তালিকা আনে।"""
    get_endpoint = f"{DATABASE_API_URL}/api/toffee/channels"
    try:
        response = requests.get(get_endpoint, timeout=20) # 20 সেকেন্ড টাইমআউট
        response.raise_for_status()
        channels = response.json()
        if isinstance(channels, list):
             logger.info(f"Successfully fetched {len(channels)} channels from database API.")
             return channels
        else:
             logger.error("Received non-list data from database API.")
             return None

    except requests.exceptions.Timeout:
         logger.error(f"Timeout error while fetching channels from database API: {get_endpoint}")
         return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching channels from database API {get_endpoint}: {e}")
        if e.response is not None:
             logger.error(f"DB API Response Status: {e.response.status_code}")
             logger.error(f"DB API Response Body: {e.response.text[:500]}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"Failed to decode JSON response from database API. Error: {json_err}")
        logger.debug(f"Response text was: {response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred fetching channels from DB: {e}")
        return None

def generate_m3u(channels):
    """চ্যানেল লিস্ট থেকে M3U প্লেলিস্ট স্ট্রিং তৈরি করে।"""
    if not channels:
        logger.warning("Cannot generate M3U: No channels provided.")
        return "#EXTM3U\n#EXTINF:-1,No Channels Found\n# Error: Could not retrieve channel list from the database."

    m3u_content = ["#EXTM3U"]
    group_title = "Toffee" # Static group title

    for channel in channels:
        name = channel.get('name', 'Unknown Channel').strip()
        logo = channel.get('logo', '').strip()
        link = channel.get('link', '').strip()

        if not link: # লিঙ্ক ছাড়া এন্ট্রি বাদ দিন
            logger.warning(f"Skipping channel '{name}' due to missing link.")
            continue

        # M3U ট্যাগ ফরম্যাট করুন
        # tvg-id="", tvg-name="", tvg-logo="", group-title=""
        extinf_line = f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}" tvg-logo="{logo}" group-title="{group_title}",{name}'
        m3u_content.append(extinf_line)
        m3u_content.append(link)

    logger.info(f"Generated M3U playlist with {len(channels)} channels.")
    return "\n".join(m3u_content)

# --- Flask Application ---
app = Flask(__name__)

# --- Scheduler Setup ---
scheduler = BackgroundScheduler(daemon=True, timezone='Asia/Dhaka') # আপনার টাইমজোন দিন
# টাস্কটি অ্যাপ স্টার্ট হওয়ার ১০ সেকেন্ড পর প্রথমবার এবং তারপর প্রতি ঘণ্টায় রান করবে
scheduler.add_job(sync_data_task, 'interval', hours=1, next_run_time=datetime.now().replace(second=10, microsecond=0))
try:
    scheduler.start()
    logger.info("Scheduler started successfully.")
    # অ্যাপ স্টার্ট হওয়ার সাথে সাথে একবার ডেটা সিঙ্ক করার চেষ্টা করুন (ঐচ্ছিক)
    # sync_data_task() # যদি চান প্রথমবার ম্যানুয়ালি কল করতে
except Exception as e:
    logger.error(f"Failed to start scheduler: {e}")

# --- Flask Routes ---

@app.route('/')
def index():
    """সার্ভার চলছে কিনা তা জানার জন্য একটি সাধারণ রুট।"""
    return jsonify({
        "status": "M3U Playlist Server is running",
        "playlist_url": "/toffee.m3u",
        "last_sync_status": "Check logs for sync details", # এখানে স্ট্যাটাস রাখা যেতে পারে
        "github_source": GITHUB_JSON_URL,
        "database_api": DATABASE_API_URL
        })

@app.route('/health')
def health_check():
    """Health check endpoint for Render."""
    return "OK", 200

@app.route('/toffee.m3u')
def serve_toffee_m3u():
    """ডাটাবেস থেকে ডেটা এনে M3U প্লেলিস্ট সার্ভ করে।"""
    logger.info("Received request for /toffee.m3u")
    channels = get_channels_from_db()

    if channels is None:
        # ডেটা আনতে ব্যর্থ হলে একটি ত্রুটি বার্তা সহ খালি প্লেলিস্ট দিন
        error_m3u = "#EXTM3U\n#EXTINF:-1,Error\n# Failed to retrieve channel list from the database server."
        logger.error("Failed to get channels from DB, returning error M3U.")
        # 503 Service Unavailable দেওয়া যেতে পারে
        return Response(error_m3u, mimetype='application/vnd.apple.mpegurl', status=503)

    m3u_playlist = generate_m3u(channels)
    return Response(m3u_playlist, mimetype='application/vnd.apple.mpegurl') # সঠিক MIME টাইপ দিন

# --- Application Cleanup ---
@app.teardown_appcontext
def shutdown_scheduler(exception=None):
    # অ্যাপ বন্ধ হওয়ার সময় শিডিউলার বন্ধ করা (যদি প্রয়োজন হয়)
    # সাধারণত Render এর ক্ষেত্রে এটি নিজে থেকেই হ্যান্ডেল হবে
    # if scheduler.running:
    #     scheduler.shutdown()
    #     logger.info("Scheduler shut down.")
    pass

# --- Run Locally (for testing) ---
# Render gunicorn ব্যবহার করবে, তাই এই অংশটি সেখানে সরাসরি রান হবে না
if __name__ == '__main__':
    # এনভায়রনমেন্ট ভেরিয়েবল সেট না থাকলে সতর্কবার্তা দিন
    if not os.environ.get('GITHUB_JSON_URL'):
        logger.warning("Environment variable 'GITHUB_JSON_URL' is not set. Using default.")
    if not os.environ.get('DATABASE_API_URL'):
        logger.warning("Environment variable 'DATABASE_API_URL' is not set. Using default.")

    # Debug মোড লোকাল টেস্টিংয়ের জন্য ভালো, Render এ False রাখুন
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
