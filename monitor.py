import asyncio
import time
import json
import os
import html
import logging
import re
import aiohttp
from datetime import datetime
from collections import OrderedDict

CONFIG_FILE = 'config.json'

def load_json(filepath, default):
    if not os.path.exists(filepath):
        return default
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return default

def save_json_atomic(filepath, data):
    tmp_path = f"{filepath}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, filepath)

config = load_json(CONFIG_FILE, {})

LOG_FILE = config.get('LOG_FILE', 'monitor.log')
STATE_FILE = config.get('STATE_FILE', 'seen_ids.json')
SCHEDULED_FILE = config.get('SCHEDULED_FILE', 'scheduled.json')
ACTIVE_LIVE_FILE = config.get('ACTIVE_LIVE_FILE', 'active_lives.json')
MAX_HISTORY = config.get('MAX_HISTORY', 1000)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

TELEGRAM_TOKEN = config.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = config.get('TELEGRAM_CHAT_ID')
TWITCH_CLIENT_ID = config.get('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET = config.get('TWITCH_CLIENT_SECRET')
FREE_CHAT_ID = config.get('FREE_CHAT_ID', '')

STALE_SCHEDULE_THRESHOLD_SEC = 10800
TARGETED_POLL_PRE_START_SEC = 300

twitch_access_token = None
twitch_token_expiry = 0

state_lock = asyncio.Lock()
scheduled_lock = asyncio.Lock()
active_lives_lock = asyncio.Lock()
telegram_lock = asyncio.Lock()
twitch_auth_lock = asyncio.Lock()

seen_ids = OrderedDict.fromkeys(load_json(STATE_FILE, []))
scheduled_streams = load_json(SCHEDULED_FILE, [])
active_lives = load_json(ACTIVE_LIVE_FILE, [])

async def save_state():
    async with state_lock:
        while len(seen_ids) > MAX_HISTORY:
            seen_ids.popitem(last=False)
        keys_to_save = list(seen_ids.keys())
    await asyncio.to_thread(save_json_atomic, STATE_FILE, keys_to_save)

async def ensure_twitch_token(session):
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
                    async with session.post(auth_url, data=payload, timeout=10) as resp:
                        resp.raise_for_status()
                        auth_data = await resp.json()
                        twitch_access_token = auth_data.get('access_token')
                        if not twitch_access_token: return False
                        twitch_token_expiry = time.time() + auth_data.get('expires_in', 0) - 60
                except Exception as e:
                    logging.error(f"Twitch Auth Error: {e}")
                    return False
    return True

async def get_twitch_stream_info(username, session):
    if await ensure_twitch_token(session):
        headers = {'Client-ID': TWITCH_CLIENT_ID, 'Authorization': f"Bearer {twitch_access_token}"}
        stream_url = f"https://api.twitch.tv/helix/streams?user_login={username}"
        try:
            async with session.get(stream_url, headers=headers, timeout=10) as resp:
                resp.raise_for_status()
                data = (await resp.json()).get('data', [])
                if data: 
                    return data[0].get('game_name', ""), data[0].get('title', "")
        except Exception as e:
            logging.error(f"Twitch API Error: {e}")
    return "", ""

async def send_telegram_notification(data, prefix, channel_name, session):
    video_id = data.get('id')
    raw_title = data.get('title', 'Unknown Title')
    raw_title = re.sub(r'\s\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$', '', raw_title)
    
    url = data.get('webpage_url', f"https://www.youtube.com/watch?v={video_id}")    
    if 'twitch.tv' in url.lower():
        match = re.search(r'twitch\.tv/([^/?]+)', url.lower())
        if match:
            username = match.group(1)
            game_name, twitch_title = await get_twitch_stream_info(username, session)
            if twitch_title: raw_title = twitch_title
            if game_name: raw_title = f"{raw_title} \u2014 {game_name}"

    time_str = ""
    sched_ts = data.get('scheduled_timestamp') or data.get('release_timestamp')
    actual_ts = data.get('timestamp')
    duration_raw = data.get('duration')

    if sched_ts:
        start_time_dt = datetime.fromtimestamp(float(sched_ts))
        time_str = f" | Time: {start_time_dt.strftime('%Y-%m-%d %H:%M')}"
    elif actual_ts and (prefix in ["VOD ARCHIVE", "VIDEO UPLOAD", "NEW SHORTS"] or "Twitch" in prefix):
        start_time_dt = datetime.fromtimestamp(float(actual_ts))
        time_str = f" | Time: {start_time_dt.strftime('%Y-%m-%d %H:%M')}"

    base_ts = sched_ts or actual_ts
    if prefix == "VOD ARCHIVE" and duration_raw and base_ts:
        end_ts = float(base_ts) + float(duration_raw)
        end_time_dt = datetime.fromtimestamp(end_ts)
        start_time_dt = datetime.fromtimestamp(float(base_ts))
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
    
    api_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    async with telegram_lock:
        try:
            async with session.post(api_url, json=payload, timeout=15) as resp:
                resp.raise_for_status()
        except Exception as e:
            logging.error(f"Telegram API Error: {e}")
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

async def process_channel(channel, allowed_tab, cookies_file, session):
    global scheduled_streams, active_lives
    state_modified = False
    channel_name = channel.get('name', 'Unknown Channel')
    base_url = channel.get('url')
    keywords = channel.get('keywords', [])
    is_twitch = 'twitch.tv' in base_url.lower()
    
    if is_twitch and allowed_tab == 'shorts': return
        
    if is_twitch:
        target_url = f"{base_url.rstrip('/')}/videos" if allowed_tab in ['videos', 'streams'] else base_url
    else:
        target_url = f"{base_url.rstrip('/')}/{allowed_tab}"
        
    items = await fetch_latest_items(target_url, cookies_file, m_type=allowed_tab)
    
    for item in items:
        v_id = item.get('id')
        if not v_id or v_id == FREE_CHAT_ID: continue
        if not check_keywords(f"{item.get('title', '')} {item.get('description', '')}", keywords): continue

        live_status = item.get('live_status')
        
        if live_status == 'is_upcoming':
            should_notify = False
            async with state_lock:
                if f"{v_id}_scheduled" not in seen_ids:
                    seen_ids[f"{v_id}_scheduled"] = None
                    state_modified = True
                    should_notify = True
                    
            if should_notify:
                prefix = "SCHEDULED - PREMIERE" if allowed_tab == 'videos' else ("SCHEDULED - Collab" if keywords else f"SCHEDULED - {'Twitch' if is_twitch else 'Youtube'}")
                await send_telegram_notification(item, prefix, channel_name, session)
                
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
                    await asyncio.to_thread(save_json_atomic, SCHEDULED_FILE, schedule_to_save)
        
        elif live_status == 'is_live':
            if is_twitch and '/videos/' in item.get('webpage_url', '').lower(): continue

            should_notify = False
            async with state_lock:
                state_marker = f"{v_id}_live"
                if state_marker not in seen_ids:
                    seen_ids[state_marker] = None
                    seen_ids.pop(f"{v_id}_scheduled", None)
                    state_modified = True
                    should_notify = True
                    
            if should_notify:
                rich_item = item
                if not is_twitch:
                    watch_url = f"https://www.youtube.com/watch?v={v_id}"
                    try:
                        deep_data = await fetch_latest_items(watch_url, cookies_file, m_type='live')
                        if deep_data: rich_item = deep_data[0]
                    except Exception as e:
                        logging.warning(f"Deep extraction failed, using shallow data: {e}")

                prefix = "PREMIERE - " + ("Collab" if keywords else "Youtube") if allowed_tab == 'videos' else ("LIVE - Collab" if keywords else f"LIVE - {'Twitch' if is_twitch else 'Youtube'}")
                await send_telegram_notification(rich_item, prefix, channel_name, session)
                
                needs_schedule_save = False
                schedule_to_save = []
                
                async with scheduled_lock:
                    original_len = len(scheduled_streams)
                    scheduled_streams = [s for s in scheduled_streams if s['id'] != v_id]
                    if len(scheduled_streams) < original_len:
                        schedule_to_save = list(scheduled_streams)
                        needs_schedule_save = True
                        
                if needs_schedule_save:
                    await asyncio.to_thread(save_json_atomic, SCHEDULED_FILE, schedule_to_save)
                    
                async with active_lives_lock:
                    if not any(s['id'] == v_id for s in active_lives):
                        active_lives.append({
                            'id': v_id,
                            'url': f"https://www.youtube.com/watch?v={v_id}" if not is_twitch else base_url,
                            'timestamp': time.time(),
                            'channel_name': channel_name,
                            'is_twitch': is_twitch
                        })
                        await asyncio.to_thread(save_json_atomic, ACTIVE_LIVE_FILE, list(active_lives))
                
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
                await send_telegram_notification(item, prefix_label, channel_name, session)
                
                async with scheduled_lock:
                    original_len = len(scheduled_streams)
                    scheduled_streams = [s for s in scheduled_streams if s['id'] != v_id]
                    if len(scheduled_streams) < original_len:
                        await asyncio.to_thread(save_json_atomic, SCHEDULED_FILE, list(scheduled_streams))

    if state_modified:
        await save_state()

async def fast_block_worker(channels, cookies_file, interval, session):
    global scheduled_streams, active_lives
    
    yt_main_live = [c for c in channels if not c.get('keywords') and 'live' in c.get('monitor', [])]
    
    queue_idx = 0
    while True:
        try:
            current_time = time.time()
            
            async with scheduled_lock:
                stale_streams = [s['id'] for s in scheduled_streams if current_time > s['timestamp'] + STALE_SCHEDULE_THRESHOLD_SEC]
                if stale_streams:
                    scheduled_streams = [s for s in scheduled_streams if s['id'] not in stale_streams]
                    await asyncio.to_thread(save_json_atomic, SCHEDULED_FILE, list(scheduled_streams))
            
            dynamic_queue = []
            for c in yt_main_live:
                dynamic_queue.append({'type': 'channel', 'data': c})
                
            async with scheduled_lock:
                for s in scheduled_streams:
                    if current_time >= s['timestamp'] - TARGETED_POLL_PRE_START_SEC:
                        dynamic_queue.append({'type': 'scheduled', 'data': s})
                        
            if not dynamic_queue:
                await asyncio.sleep(interval)
                continue
                
            if queue_idx >= len(dynamic_queue):
                queue_idx = 0
                
            current_item = dynamic_queue[queue_idx]
            
            if current_item['type'] == 'scheduled':
                stream = current_item['data']
                v_id = stream['id']
                logging.info(f"Fast Scan (scheduled): '{stream.get('channel_name')}'")
                
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
                            
                        await send_telegram_notification(items[0], prefix_label, stream['channel_name'], session)
                            
                    if needs_state_save:
                        await save_state()
                        
                    async with scheduled_lock:
                        scheduled_streams = [s for s in scheduled_streams if s['id'] != v_id]
                        await asyncio.to_thread(save_json_atomic, SCHEDULED_FILE, list(scheduled_streams))
                        
                    async with active_lives_lock:
                        if not any(s['id'] == v_id for s in active_lives):
                            active_lives.append({
                                'id': v_id,
                                'url': stream['url'],
                                'timestamp': time.time(),
                                'channel_name': stream['channel_name'],
                                'is_twitch': stream.get('is_twitch', False)
                            })
                            await asyncio.to_thread(save_json_atomic, ACTIVE_LIVE_FILE, list(active_lives))
                            
            elif current_item['type'] == 'channel':
                target_channel = current_item['data']
                logging.info(f"Fast Scan (live): '{target_channel.get('name')}'")
                await process_channel(target_channel, 'live', cookies_file, session)

        except Exception as e:
            logging.error(f"Fast Block Error: {e}")
            
        queue_idx += 1
        await asyncio.sleep(interval)

async def rolling_queue_worker(queue_name, channels, tabs, interval, cookies_file, session):
    global active_lives
    if not channels: return
    
    static_queue = []
    for c in channels:
        monitored_tabs = c.get('monitor', ['streams'])
        for t in tabs:
            if t in monitored_tabs:
                static_queue.append((c, t))
                
    if not static_queue: return
        
    queue_idx = 0
    while True:
        dynamic_queue = list(static_queue)
        
        if queue_name == "Main":
            async with active_lives_lock:
                for s in active_lives:
                    dynamic_queue.append({'type': 'active_live', 'data': s})
                    
        if queue_idx >= len(dynamic_queue):
            queue_idx = 0
            
        current_item = dynamic_queue[queue_idx]
        
        if isinstance(current_item, dict) and current_item.get('type') == 'active_live':
            stream = current_item['data']
            v_id = stream['id']
            logging.info(f"{queue_name} Scan (active_live): '{stream.get('channel_name')}'")
            
            if time.time() > stream['timestamp'] + 10800:
                async with active_lives_lock:
                    active_lives[:] = [s for s in active_lives if s['id'] != v_id]
                    await asyncio.to_thread(save_json_atomic, ACTIVE_LIVE_FILE, list(active_lives))
            else:
                try:
                    items = await fetch_latest_items(stream['url'], cookies_file, m_type='targeted')
                    if items:
                        live_status = items[0].get('live_status')
                        if live_status == 'was_live' or (live_status is None and items[0].get('duration')):
                            needs_state_save = False
                            should_notify_vod = False
                            async with state_lock:
                                if f"{v_id}_vod" not in seen_ids:
                                    seen_ids[f"{v_id}_vod"] = None
                                    seen_ids.pop(f"{v_id}_live", None)
                                    needs_state_save = True
                                    should_notify_vod = True
                            if should_notify_vod:
                                await send_telegram_notification(items[0], "VOD ARCHIVE", stream['channel_name'], session)
                            if needs_state_save:
                                await save_state()
                                
                            async with active_lives_lock:
                                active_lives[:] = [s for s in active_lives if s['id'] != v_id]
                                await asyncio.to_thread(save_json_atomic, ACTIVE_LIVE_FILE, list(active_lives))
                    else:
                        if stream.get('is_twitch'):
                            async with active_lives_lock:
                                active_lives[:] = [s for s in active_lives if s['id'] != v_id]
                                await asyncio.to_thread(save_json_atomic, ACTIVE_LIVE_FILE, list(active_lives))
                except Exception as e:
                    logging.error(f"Integrated Live Finish Error: {e}")
        else:
            target_channel, current_tab = current_item
            logging.info(f"{queue_name} Scan ({current_tab}): '{target_channel.get('name')}'")
            try:
                await process_channel(target_channel, current_tab, cookies_file, session)
            except Exception as e:
                logging.error(f"{queue_name} Task Error for '{target_channel.get('name')}' on tab '{current_tab}': {e}")
        
        queue_idx += 1
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

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(fast_block_worker(channels, cookies_file, live_interval, session)),
            asyncio.create_task(rolling_queue_worker("Main", yt_main, ['streams', 'videos', 'shorts'], main_interval, cookies_file, session)),
            asyncio.create_task(rolling_queue_worker("Collab", yt_collab, ['live', 'streams', 'videos', 'shorts'], collab_interval, cookies_file, session))
        ]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
