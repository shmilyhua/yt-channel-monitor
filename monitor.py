import asyncio
import time
import json
import os
import requests
import html
import logging
import re
from datetime import datetime

LOG_FILE = 'monitor.log'
CONFIG_FILE = 'config.json'
STATE_FILE = 'seen_ids.json'
SCHEDULED_FILE = 'scheduled.json'
MAX_HISTORY = 1000

STALE_SCHEDULE_THRESHOLD_SEC = 21600
TARGETED_POLL_PRE_START_SEC = 300

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- LOCKS FOR CONCURRENT OPERATIONS ---
state_lock = asyncio.Lock()
scheduled_lock = asyncio.Lock()
telegram_lock = asyncio.Lock()
twitch_auth_lock = asyncio.Lock()

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

# In-memory state tracking to prevent disk thrashing
seen_ids = dict.fromkeys(load_json(STATE_FILE, []))
scheduled_streams = load_json(SCHEDULED_FILE, [])

# --- ASYNC HELPER FUNCTIONS ---
async def save_state():
    async with state_lock:
        while len(seen_ids) > MAX_HISTORY:
            seen_ids.pop(next(iter(seen_ids)))
        keys_to_save = list(seen_ids.keys())
        
    await asyncio.to_thread(save_json, STATE_FILE, keys_to_save)

async def ensure_twitch_token():
    global twitch_access_token, twitch_token_expiry
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return False

    if not twitch_access_token or time.time() >= twitch_token_expiry:
        async with twitch_auth_lock:
            if not twitch_access_token or time.time() >= twitch_token_expiry:
                auth_url = "https://id.twitch.tv/oauth2/token"
                payload = {
                    'client_id': TWITCH_CLIENT_ID,
                    'client_secret': TWITCH_CLIENT_SECRET,
                    'grant_type': 'client_credentials'
                }
                try:
                    auth_res = await asyncio.to_thread(requests.post, auth_url, data=payload, timeout=10)
                    auth_data = auth_res.json()
                    twitch_access_token = auth_data.get('access_token')
                    if not twitch_access_token:
                        return False
                    twitch_token_expiry = time.time() + auth_data.get('expires_in', 0) - 60
                except Exception as e:
                    logging.error(f"Twitch Auth Error: {e}")
                    return False
    return True

def get_twitch_stream_info_sync(username, token):
    if not token:
        return "", ""
    headers = {'Client-ID': TWITCH_CLIENT_ID, 'Authorization': f"Bearer {token}"}
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
    if await ensure_twitch_token():
        return await asyncio.to_thread(get_twitch_stream_info_sync, username, twitch_access_token)
    return "", ""

def send_telegram_sync(payload):
    api_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    try:
        response = requests.post(api_url, json=payload, timeout=15)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Telegram API Error: {e}")

async def send_telegram_notification(data, prefix, channel_name):
    video_id = data.get('id')
    raw_title = data.get('title', 'Unknown Title')
    
    raw_title = re.sub(r'\s\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$', '', raw_title)
    
    url = data.get('webpage_url', f"https://www.youtube.com/watch?v={video_id}")    
    if 'twitch.tv' in url.lower():
        match = re.search(r'twitch\.tv/([^/?]+)', url.lower())
        if match:
            username = match.group(1)
            game_name, twitch_title = await get_twitch_stream_info(username)
            
            if twitch_title:
                raw_title = twitch_title
                
            if game_name:
                raw_title = f"{raw_title} \u2014 {game_name}"

    time_str = ""
    sched_ts = data.get('scheduled_timestamp') or data.get('release_timestamp')
    actual_ts = data.get('timestamp')
    duration_raw = data.get('duration')

    if "LIVE" in prefix:
        best_ts = None
    elif "SCHEDULED" in prefix:
        best_ts = sched_ts or actual_ts
    else:
        best_ts = actual_ts or sched_ts

    if best_ts:
        start_time_dt = datetime.fromtimestamp(float(best_ts))
        time_str = f" | Time: {start_time_dt.strftime('%Y-%m-%d %H:%M')}"
        
        if prefix == "VOD ARCHIVE" and duration_raw:
            end_ts = float(best_ts) + float(duration_raw)
            end_time_dt = datetime.fromtimestamp(end_ts)
            
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
    
    async with telegram_lock:
        await asyncio.to_thread(send_telegram_sync, payload)
        await asyncio.sleep(1.5)

async def fetch_latest_items(target_url, master_cookies_file, m_type=''):
    items_range = '1-3' if m_type == 'streams' else ('1' if m_type in ['live', 'targeted'] else '1-2')
    
    cmd = [
        'yt-dlp', 
        '-j', 
        '--no-warnings', 
        '--playlist-items', items_range, 
        '--ignore-no-formats-error'
    ]
    
    if master_cookies_file and os.path.exists(master_cookies_file):
        cmd.extend(['--cookies', master_cookies_file])
        
    cmd.append(target_url)
    
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
        await process.wait()
        logging.warning(f"Timeout fetching data for {target_url}")
    except Exception as e:
        logging.error(f"Async execution error for {target_url}: {e}")
            
    return items

def check_keywords(text, keywords):
    if not keywords: return True
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)

# --- CORE PROCESSING LOGIC ---
async def process_channel(channel, allowed_tab, cookies_file):
    global scheduled_streams
    state_modified = False
    channel_name = channel.get('name', 'Unknown Channel')
    base_url = channel.get('url')
    keywords = channel.get('keywords', [])
    is_twitch = 'twitch.tv' in base_url.lower()
    
    if is_twitch and allowed_tab == 'shorts':
        return
        
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

        live_status = item.get('live_status')
        
        # 1. UPCOMING STATUS
        if live_status == 'is_upcoming':
            should_notify = False
            async with state_lock:
                if f"{v_id}_scheduled" not in seen_ids:
                    seen_ids[f"{v_id}_scheduled"] = None
                    state_modified = True
                    should_notify = True
                    
            if should_notify:
                if allowed_tab == 'videos':
                    prefix = "SCHEDULED - PREMIERE" if keywords else "SCHEDULED - PREMIERE"
                else:
                    prefix = "SCHEDULED - Collab" if keywords else f"SCHEDULED - {'Twitch' if is_twitch else 'Youtube'}"
                    
                await send_telegram_notification(item, prefix, channel_name)
                
                raw_sched = item.get('scheduled_timestamp') or item.get('release_timestamp')
                fallback = time.time() + 31536000
                
                needs_schedule_save = False
                schedule_to_save = []
                
                async with scheduled_lock:
                    if not any(s['id'] == v_id for s in scheduled_streams):
                        scheduled_streams.append({
                            'id': v_id,
                            'url': item.get('webpage_url', f"https://www.youtube.com/watch?v={v_id}"),
                            'timestamp': float(raw_sched) if raw_sched else fallback,
                            'channel_name': channel_name,
                            'is_collab': bool(keywords),
                            'is_twitch': is_twitch,
                            'is_premiere': allowed_tab == 'videos'
                        })
                        schedule_to_save = list(scheduled_streams)
                        needs_schedule_save = True
                        
                if needs_schedule_save:
                    await asyncio.to_thread(save_json, SCHEDULED_FILE, schedule_to_save)
        
        # 2. LIVE STATUS
        elif live_status == 'is_live':
            if is_twitch and '/videos/' in item.get('webpage_url', '').lower():
                continue

            should_notify = False
            async with state_lock:
                state_marker = f"{v_id}_live"
                if state_marker not in seen_ids:
                    seen_ids[state_marker] = None
                    seen_ids.pop(f"{v_id}_scheduled", None)
                    state_modified = True
                    should_notify = True
                    
            if should_notify:
                if allowed_tab == 'videos':
                    prefix = "PREMIERE - Collab" if keywords else "PREMIERE - Youtube"
                else:
                    prefix = "LIVE - Collab" if keywords else f"LIVE - {'Twitch' if is_twitch else 'Youtube'}"
                    
                await send_telegram_notification(item, prefix, channel_name)
                
                needs_schedule_save = False
                schedule_to_save = []
                
                async with scheduled_lock:
                    original_len = len(scheduled_streams)
                    scheduled_streams = [s for s in scheduled_streams if s['id'] != v_id]
                    if len(scheduled_streams) < original_len:
                        schedule_to_save = list(scheduled_streams)
                        needs_schedule_save = True
                        
                if needs_schedule_save:
                    await asyncio.to_thread(save_json, SCHEDULED_FILE, schedule_to_save)
                
        # 3. PAST / VOD / STANDARD UPLOADS
        else:
            should_notify = False
            prefix_label = ""
            
            async with state_lock:
                if allowed_tab in ['videos', 'shorts']:
                    if f"{v_id}_live" in seen_ids and allowed_tab == 'videos':
                        if v_id not in seen_ids:
                            seen_ids[v_id] = None
                            seen_ids.pop(f"{v_id}_live", None)
                            state_modified = True
                            should_notify = True
                            prefix_label = "VIDEO UPLOAD"
                    elif not any(f"{v_id}{suffix}" in seen_ids for suffix in ['', '_scheduled', '_live', '_vod']):
                        seen_ids[v_id] = None
                        state_modified = True
                        should_notify = True
                        prefix_label = "NEW SHORTS" if allowed_tab == 'shorts' else "VIDEO UPLOAD"
                else:
                    state_marker = f"{v_id}_vod"
                    if state_marker not in seen_ids:
                        seen_ids[state_marker] = None
                        seen_ids.pop(f"{v_id}_scheduled", None)
                        seen_ids.pop(f"{v_id}_live", None)
                        state_modified = True
                        should_notify = True
                        prefix_label = "VOD ARCHIVE"
            
            if should_notify:
                await send_telegram_notification(item, prefix_label, channel_name)

    if state_modified:
        await save_state()

# --- ASYNC WORKERS ---
async def fast_block_worker(channels, cookies_file, interval):
    global scheduled_streams
    yt_main = [c for c in channels if not c.get('keywords')]
    
    while True:
        try:
            # 1. Snapshot queue in memory
            async with scheduled_lock:
                current_queue = list(scheduled_streams)
                
            queue_modified = False
            current_time = time.time()
            streams_to_remove = set()

            # 2. Iterate snapshot outside the lock
            for stream in sorted(current_queue, key=lambda x: x.get('timestamp', 0)):
                v_id = stream['id']
                if current_time > stream['timestamp'] + STALE_SCHEDULE_THRESHOLD_SEC:
                    logging.info(f"Dropping stale stream: {v_id}")
                    streams_to_remove.add(v_id)
                    queue_modified = True
                    continue

                if current_time >= stream['timestamp'] - TARGETED_POLL_PRE_START_SEC:
                    logging.info(f"Polling targeted schedule: {v_id}")
                    items = await fetch_latest_items(stream['url'], cookies_file, m_type='targeted')
                    if items and items[0].get('live_status') == 'is_live':
                        needs_state_save = False
                        should_notify_targeted = False
                        
                        async with state_lock:
                            if f"{v_id}_live" not in seen_ids:
                                seen_ids[f"{v_id}_live"] = None
                                seen_ids.pop(f"{v_id}_scheduled", None)
                                needs_state_save = True
                                should_notify_targeted = True
                                
                        if should_notify_targeted:
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
                                
                        if needs_state_save:
                            await save_state()
                            
                        streams_to_remove.add(v_id)
                        queue_modified = True

            # 3. Re-acquire lock to apply deletions (preserving concurrent additions)
            if queue_modified:
                async with scheduled_lock:
                    scheduled_streams = [s for s in scheduled_streams if s['id'] not in streams_to_remove]
                    schedule_to_save = list(scheduled_streams)
                    
                await asyncio.to_thread(save_json, SCHEDULED_FILE, schedule_to_save)

            tasks = []
            for c in yt_main:
                if 'live' in c.get('monitor', []):
                    tasks.append(process_channel(c, 'live', cookies_file))
                    
            if tasks: await asyncio.gather(*tasks)
            
        except Exception as e:
            logging.error(f"Fast Block Error: {e}")
            
        await asyncio.sleep(interval)

async def rolling_queue_worker(queue_name, channels, tabs, interval, cookies_file):
    if not channels: return
    
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
        try:
            await process_channel(target_channel, current_tab, cookies_file)
        except Exception as e:
            logging.error(f"{queue_name} Task Error for '{target_channel.get('name')}' on tab '{current_tab}': {e}")
        
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
    
    yt_main = [c for c in channels if not c.get('keywords')]
    yt_collab = [c for c in channels if c.get('keywords')]

    logging.info(f"Async Monitor active. Main Interval: {main_interval}s | Collab Interval: {collab_interval}s")

    tasks = [
        asyncio.create_task(fast_block_worker(channels, cookies_file, live_interval)),
        asyncio.create_task(rolling_queue_worker("Main", yt_main, ['streams', 'videos', 'shorts'], main_interval, cookies_file)),
        asyncio.create_task(rolling_queue_worker("Collab", yt_collab, ['live', 'streams', 'videos', 'shorts'], collab_interval, cookies_file))
    ]
    
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
