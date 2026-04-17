import asyncio
import time
import json
import os
import requests
import html
import shutil
import logging
import re
from datetime import datetime

LOG_FILE = 'monitor.log'
CONFIG_FILE = 'config.json'
STATE_FILE = 'seen_ids.json'
SCHEDULED_FILE = 'scheduled.json'
MAX_HISTORY = 1000

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- LOCKS FOR CONCURRENT WRITES ---
state_lock = asyncio.Lock()
scheduled_lock = asyncio.Lock()

def load_json(filepath, default):
    if not os.path.exists(filepath):
        return default
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return default

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

config = load_json(CONFIG_FILE, {})
TELEGRAM_TOKEN = config.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = config.get('TELEGRAM_CHAT_ID')
TWITCH_CLIENT_ID = config.get('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET = config.get('TWITCH_CLIENT_SECRET')
FREE_CHAT_ID = config.get('FREE_CHAT_ID', '')

twitch_access_token = None
twitch_token_expiry = 0
seen_ids_set = set(load_json(STATE_FILE, []))

# --- ASYNC HELPER FUNCTIONS ---
async def save_state():
    async with state_lock:
        capped_state = list(seen_ids_set)[-MAX_HISTORY:] if len(seen_ids_set) > MAX_HISTORY else list(seen_ids_set)
        await asyncio.to_thread(save_json, STATE_FILE, capped_state)

def get_twitch_stream_info_sync(username):
    global twitch_access_token, twitch_token_expiry
    current_time = time.time()
    
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return "", ""

    if not twitch_access_token or current_time >= twitch_token_expiry:
        auth_url = "https://id.twitch.tv/oauth2/token"
        payload = {
            'client_id': TWITCH_CLIENT_ID,
            'client_secret': TWITCH_CLIENT_SECRET,
            'grant_type': 'client_credentials'
        }
        try:
            auth_res = requests.post(auth_url, data=payload, timeout=10).json()
            twitch_access_token = auth_res.get('access_token')
            if not twitch_access_token:
                return "", ""
            twitch_token_expiry = current_time + auth_res.get('expires_in', 0) - 60
        except Exception as e:
            logging.error(f"Twitch Auth Error: {e}")
            return "", ""

    headers = {'Client-ID': TWITCH_CLIENT_ID, 'Authorization': f"Bearer {twitch_access_token}"}
    try:
        stream_url = f"https://api.twitch.tv/helix/streams?user_login={username}"
        stream_res = requests.get(stream_url, headers=headers, timeout=10).json()
        data = stream_res.get('data', [])
        if data: 
            return data[0].get('game_name', ""), data[0].get('title', "")
    except Exception as e:
        logging.error(f"Twitch API Error: {e}")
    return "", ""

async def get_twitch_stream_info(username):
    return await asyncio.to_thread(get_twitch_stream_info_sync, username)

def send_telegram_sync(payload):
    api_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    try:
        requests.post(api_url, json=payload, timeout=15)
        time.sleep(1.5) # Built-in rate limit protection
    except Exception as e:
        logging.error(f"Telegram API Error: {e}")

async def send_telegram_notification(data, prefix, channel_name):
    video_id = data.get('id')
    raw_title = data.get('title', 'Unknown Title')
    
    # Strip trailing yt-dlp timestamp fallback (e.g., " 2026-04-17 23:01")
    raw_title = re.sub(r'\s\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$', '', raw_title)
    
    url = data.get('webpage_url', f"https://www.youtube.com/watch?v={video_id}")    
    if 'twitch.tv' in url.lower():
        match = re.search(r'twitch\.tv/([^/?]+)', url.lower())
        if match:
            username = match.group(1)
            # Fetch both game name and title from the updated API function
            game_name, twitch_title = await get_twitch_stream_info(username)
            
            # Overwrite the yt-dlp generic title with the official Twitch title
            if twitch_title:
                raw_title = twitch_title
                
            if game_name:
                raw_title = f"{raw_title} \u2014 {game_name}"

    time_str = ""
    sched_ts = data.get('scheduled_timestamp') or data.get('release_timestamp')
    actual_ts = data.get('timestamp')
    duration_raw = data.get('duration')

    # Prioritize scheduled/release timestamp to show correct live times, fallback to actual upload timestamp
    best_ts = sched_ts if sched_ts else actual_ts

    if best_ts:
        start_time_dt = datetime.fromtimestamp(float(best_ts))
        time_str = f" | Time: {start_time_dt.strftime('%Y-%m-%d %H:%M')}"
        
        # Calculate and append finish time specifically for VODs
        if prefix == "VOD ARCHIVE" and duration_raw:
            end_ts = float(best_ts) + float(duration_raw)
            end_time_dt = datetime.fromtimestamp(end_ts)
            
            # Format conditionally: omits the date if the stream started and ended on the same day
            if start_time_dt.date() == end_time_dt.date():
                time_str += f" \u2014 Ended: {end_time_dt.strftime('%H:%M')}"
            else:
                time_str += f" \u2014 Ended: {end_time_dt.strftime('%m-%d %H:%M')}"

    duration_str = ""
    if duration_raw:
        m, s = divmod(int(duration_raw), 60)
        h, m = divmod(m, 60)
        duration_str = f"{h}:{m:02d}:{s:02d}" if h > 0 else f"{m}:{s:02d}"

    full_prefix = f"[{prefix}]{time_str}"
    caption_parts = [f"<b>{full_prefix}</b>", html.escape(raw_title)]
    if duration_str: caption_parts.append(duration_str)
    caption_parts.extend([html.escape(channel_name), url])
    
    thumb = data.get('thumbnail', '')
    if thumb: thumb = f"{thumb}{'&' if '?' in thumb else '?'}t={int(time.time())}"

    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'photo': thumb,
        'caption': '\n'.join(caption_parts),
        'parse_mode': 'HTML'
    }
    
    logging.info(f"Notification queued: {raw_title}")
    await asyncio.to_thread(send_telegram_sync, payload)

async def fetch_latest_items(target_url, master_cookies_file, m_type=''):
    temp_cookies = f"{master_cookies_file}.{time.time()}.tmp"
    if os.path.exists(master_cookies_file):
        await asyncio.to_thread(shutil.copy2, master_cookies_file, temp_cookies)
    else:
        temp_cookies = master_cookies_file
        
    items_range = '1-3' if m_type == 'streams' else ('1' if m_type in ['live', 'targeted'] else '1-2')
    cmd = ['yt-dlp', '--cookies', temp_cookies, '-j', '--no-warnings', '--playlist-items', items_range, '--ignore-no-formats-error', target_url]
    
    items = []
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
        
        if stdout:
            for line in stdout.decode().strip().split('\n'):
                if line: items.append(json.loads(line))
                
        if process.returncode != 0 and stderr:
            error_msg = stderr.decode().strip()
            if "Sign in to confirm" in error_msg or "cookies" in error_msg.lower():
                logging.error(f"CRITICAL: Cookie expired for {target_url}.")
    except asyncio.TimeoutError:
        process.kill()
        logging.warning(f"Timeout fetching data for {target_url}")
    except Exception as e:
        logging.error(f"Async execution error for {target_url}: {e}")
    finally:
        if temp_cookies != master_cookies_file and os.path.exists(temp_cookies):
            await asyncio.to_thread(os.remove, temp_cookies)
            
    return items

def check_keywords(text, keywords):
    if not keywords: return True
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)

# --- CORE PROCESSING LOGIC ---
async def process_channel(channel, allowed_tab, cookies_file):
    state_modified = False
    channel_name = channel.get('name', 'Unknown Channel')
    base_url = channel.get('url')
    keywords = channel.get('keywords', [])
    is_twitch = 'twitch.tv' in base_url.lower()
    
    # Twitch has no 'shorts' tab. Exit early to save network requests.
    if is_twitch and allowed_tab == 'shorts':
        return
        
    # Route Twitch VODs correctly to their video page
    if is_twitch:
        if allowed_tab in ['videos', 'streams']:
            target_url = f"{base_url.rstrip('/')}/videos"
        else:
            target_url = base_url
    else:
        target_url = f"{base_url.rstrip('/')}/{allowed_tab}"
        
    items = await fetch_latest_items(target_url, cookies_file, m_type=allowed_tab)
    
    for item in items:
        v_id = item.get('id')
        if not v_id or v_id == FREE_CHAT_ID: continue
        if not check_keywords(f"{item.get('title', '')} {item.get('description', '')}", keywords): continue

        async with state_lock:
            live_status = item.get('live_status')
            
            # 1. UPCOMING STATUS
            if live_status == 'is_upcoming':
                if f"{v_id}_scheduled" not in seen_ids_set:
                    if allowed_tab == 'videos':
                        prefix = "SCHEDULED - PREMIERE" if keywords else "SCHEDULED - PREMIERE"
                    else:
                        prefix = "SCHEDULED - Collab" if keywords else f"SCHEDULED - {'Twitch' if is_twitch else 'Youtube'}"
                        
                    await send_telegram_notification(item, prefix, channel_name)
                    seen_ids_set.add(f"{v_id}_scheduled")
                    state_modified = True
                    
                    raw_sched = item.get('scheduled_timestamp') or item.get('release_timestamp')
                    fallback = time.time() + 31536000
                    
                    async with scheduled_lock:
                        sched_list = await asyncio.to_thread(load_json, SCHEDULED_FILE, [])
                        if not any(s['id'] == v_id for s in sched_list):
                            sched_list.append({
                                'id': v_id,
                                'url': item.get('webpage_url', f"https://www.youtube.com/watch?v={v_id}"),
                                'timestamp': float(raw_sched) if raw_sched else fallback,
                                'channel_name': channel_name,
                                'is_collab': bool(keywords),
                                'is_twitch': is_twitch,
                                'is_premiere': allowed_tab == 'videos'
                            })
                            await asyncio.to_thread(save_json, SCHEDULED_FILE, sched_list)
            
            # 2. LIVE STATUS
            elif live_status == 'is_live':
                # Bypass duplicate notifications for ongoing Twitch VODs
                if is_twitch and '/videos/' in item.get('webpage_url', '').lower():
                    continue

                state_marker = f"{v_id}_live"
                if state_marker not in seen_ids_set:
                    if allowed_tab == 'videos':
                        prefix = "PREMIERE - Collab" if keywords else "PREMIERE - Youtube"
                    else:
                        prefix = "LIVE - Collab" if keywords else f"LIVE - {'Twitch' if is_twitch else 'Youtube'}"
                        
                    await send_telegram_notification(item, prefix, channel_name)
                    seen_ids_set.add(state_marker)
                    seen_ids_set.discard(f"{v_id}_scheduled") # Remove old state
                    state_modified = True
                    
                    # Ensure it is removed from scheduled.json if detected here
                    async with scheduled_lock:
                        sched_list = await asyncio.to_thread(load_json, SCHEDULED_FILE, [])
                        original_len = len(sched_list)
                        sched_list = [s for s in sched_list if s['id'] != v_id]
                        if len(sched_list) < original_len:
                            await asyncio.to_thread(save_json, SCHEDULED_FILE, sched_list)
                    
            # 3. PAST / VOD / STANDARD UPLOADS
            else:
                if allowed_tab in ['videos', 'shorts']:
                    # Intercept finished premieres and transition them to Video Uploads
                    if f"{v_id}_live" in seen_ids_set and allowed_tab == 'videos':
                        if v_id not in seen_ids_set:
                            await send_telegram_notification(item, "VIDEO UPLOAD", channel_name)
                            seen_ids_set.add(v_id)
                            seen_ids_set.discard(f"{v_id}_live")
                            state_modified = True
                        continue

                    # Standard upload filter and logic
                    if any(f"{v_id}{suffix}" in seen_ids_set for suffix in ['', '_scheduled', '_live', '_vod']): continue
                    prefix = "NEW SHORTS" if allowed_tab == 'shorts' else "VIDEO UPLOAD"
                    await send_telegram_notification(item, prefix, channel_name)
                    seen_ids_set.add(v_id)
                    state_modified = True
                else:
                    state_marker = f"{v_id}_vod"
                    if state_marker not in seen_ids_set:
                        prefix = "VOD ARCHIVE"
                        await send_telegram_notification(item, prefix, channel_name)
                        seen_ids_set.add(state_marker)
                        seen_ids_set.discard(f"{v_id}_scheduled") # Clean up old markers
                        seen_ids_set.discard(f"{v_id}_live")
                        state_modified = True

    if state_modified:
        await save_state()

# --- ASYNC WORKERS ---
async def fast_block_worker(channels, cookies_file, interval):
    # Treat Twitch as standard main channels
    yt_main = [c for c in channels if not c.get('keywords')]
    
    while True:
        try:
            # 1. Scheduled Queue Check
            async with scheduled_lock:
                scheduled_streams = await asyncio.to_thread(load_json, SCHEDULED_FILE, [])
                updated_queue = []
                queue_modified = False
                current_time = time.time()

                for stream in sorted(scheduled_streams, key=lambda x: x.get('timestamp', 0)):
                    v_id = stream['id']
                    if current_time > stream['timestamp'] + 21600:
                        logging.info(f"Dropping stale stream: {v_id}")
                        queue_modified = True
                        continue

                    if current_time >= stream['timestamp'] - 300:
                        logging.info(f"Polling targeted schedule: {v_id}")
                        items = await fetch_latest_items(stream['url'], cookies_file, m_type='targeted')
                        if items and items[0].get('live_status') == 'is_live':
                            needs_state_save = False
                            async with state_lock:
                                if f"{v_id}_live" not in seen_ids_set:
                                    
                                    # Dynamically reconstruct the prefix
                                    is_collab = stream.get('is_collab', False)
                                    is_twitch = stream.get('is_twitch', False)
                                    is_premiere = stream.get('is_premiere', False)
                                    
                                    if is_premiere:
                                        prefix_label = "PREMIERE - Collab" if is_collab else "PREMIERE - Youtube"
                                    elif is_collab:
                                        prefix_label = "LIVE - Collab"
                                    else:
                                        prefix_label = f"LIVE - {'Twitch' if is_twitch else 'Youtube'}"
                                        
                                    await send_telegram_notification(items[0], prefix_label, stream['channel_name'])
                                    seen_ids_set.add(f"{v_id}_live")
                                    seen_ids_set.discard(f"{v_id}_scheduled") # Remove old state
                                    needs_state_save = True
                                    
                            if needs_state_save:
                                await save_state()
                                
                            queue_modified = True
                            continue

                    updated_queue.append(stream)

                if queue_modified:
                    await asyncio.to_thread(save_json, SCHEDULED_FILE, updated_queue)

            # 2. Main Live / Twitch Checks
            tasks = []
            for c in yt_main:
                is_twitch = 'twitch.tv' in c.get('url', '').lower()
                if 'live' in c.get('monitor', []):
                    tasks.append(process_channel(c, 'live', cookies_file))
                    
            if tasks: await asyncio.gather(*tasks)
            
        except Exception as e:
            logging.error(f"Fast Block Error: {e}")
            
        await asyncio.sleep(interval)

async def rolling_queue_worker(queue_name, channels, tabs, interval, cookies_file):
    if not channels: return
    
    # Pre-calculate a flat list of only valid (channel, tab) pairs
    execution_queue = []
    for c in channels:
        monitored_tabs = c.get('monitor', ['streams'])
        for t in tabs:
            if t in monitored_tabs:
                execution_queue.append((c, t))
                
    if not execution_queue: 
        logging.warning(f"No valid tabs to monitor in {queue_name} queue.")
        return
        
    queue_idx = 0
    while True:
        target_channel, current_tab = execution_queue[queue_idx]
        
        logging.info(f"{queue_name} Scan ({current_tab}): '{target_channel.get('name')}'")
        await process_channel(target_channel, current_tab, cookies_file)
        
        queue_idx = (queue_idx + 1) % len(execution_queue)
        await asyncio.sleep(interval)

async def main():
    if not config:
        logging.error("Invalid or missing config.json")
        return
        
    cookies_file = config.get('COOKIES_FILE', 'www.youtube.com_cookies.txt')
    channels = config.get('CHANNELS', [])
    live_interval = config.get('LIVE_CHECK_INTERVAL', 60)
    main_interval = config.get('MAIN_SCAN_INTERVAL', 60)
    collab_interval = config.get('COLLAB_SCAN_INTERVAL', 60)
    
    # Remove the Twitch exclusions so they join the rolling queues
    yt_main = [c for c in channels if not c.get('keywords')]
    yt_collab = [c for c in channels if c.get('keywords')]

    logging.info(f"Async Monitor active. Main Interval: {main_interval}s | Collab Interval: {collab_interval}s")

    # Spawn independent workers
    tasks = [
        asyncio.create_task(fast_block_worker(channels, cookies_file, live_interval)),
        asyncio.create_task(rolling_queue_worker("Main", yt_main, ['streams', 'videos', 'shorts'], main_interval, cookies_file)),
        asyncio.create_task(rolling_queue_worker("Collab", yt_collab, ['live', 'streams', 'videos', 'shorts'], collab_interval, cookies_file))
    ]
    
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
