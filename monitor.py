import asyncio
import time
import json
import os
import html
import logging
import re
import copy
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

def write_string_atomic(filepath, data_str):
    tmp_path = f"{filepath}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        f.write(data_str)
    os.replace(tmp_path, filepath)

class YTChannelMonitor:
    def __init__(self, config):
        self.config = config
        self.log_file = config.get('LOG_FILE', 'monitor.log')
        self.state_file = config.get('STATE_FILE', 'seen_ids.json')
        self.scheduled_file = config.get('SCHEDULED_FILE', 'scheduled.json')
        self.active_live_file = config.get('ACTIVE_LIVE_FILE', 'active_lives.json')
        self.max_history = config.get('MAX_HISTORY', 1000)
        self.max_concurrent_scans = config.get('MAX_CONCURRENT_SCANS', 3)
        
        self.telegram_token = config.get('TELEGRAM_TOKEN')
        self.telegram_chat_id = config.get('TELEGRAM_CHAT_ID')
        self.telegram_admin_id = config.get('TELEGRAM_ADMIN_ID', self.telegram_chat_id)
        self.twitch_client_id = config.get('TWITCH_CLIENT_ID')
        self.twitch_client_secret = config.get('TWITCH_CLIENT_SECRET')
        self.free_chat_id = config.get('FREE_CHAT_ID', '')
        self.cookies_file = config.get('COOKIES_FILE', 'www.youtube.com_cookies.txt')

        self.seen_ids = OrderedDict.fromkeys(load_json(self.state_file, []))
        
        sched_data = load_json(self.scheduled_file, [])
        self.scheduled_streams = {s['id']: s for s in sched_data} if isinstance(sched_data, list) else sched_data
        
        active_data = load_json(self.active_live_file, [])
        self.active_lives = {s['id']: s for s in active_data} if isinstance(active_data, list) else active_data

        self.twitch_access_token = None
        self.twitch_token_expiry = 0
        self.twitch_auth_lock = asyncio.Lock()
        self.twitch_cache = {}  # {username: (game_name, title, expiry_timestamp)}
        
        self.state_io_lock = asyncio.Lock()
        self.scheduled_io_lock = asyncio.Lock()
        self.active_io_lock = asyncio.Lock()

        self.scan_semaphore = asyncio.Semaphore(self.max_concurrent_scans)
        self.notification_queue = asyncio.Queue()
        self.session = None
        self.cookies_valid = True

        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(message)s',
            datefmt='%H:%M:%S',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler()
            ],
            force=True
        )

    def _mark_seen(self, v_id, suffix=""):
        marker = f"{v_id}{suffix}"
        self.seen_ids[marker] = None
        while len(self.seen_ids) > self.max_history:
            self.seen_ids.popitem(last=False)

    async def save_state(self):
        async with self.state_io_lock:
            keys_to_save = list(self.seen_ids.keys())
            json_str = json.dumps(keys_to_save, indent=2)
            await asyncio.to_thread(write_string_atomic, self.state_file, json_str)

    async def save_scheduled(self):
        async with self.scheduled_io_lock:
            data = list(self.scheduled_streams.values())
            json_str = json.dumps(data, indent=2)
            await asyncio.to_thread(write_string_atomic, self.scheduled_file, json_str)

    async def save_active_lives(self):
        async with self.active_io_lock:
            data = list(self.active_lives.values())
            json_str = json.dumps(data, indent=2)
            await asyncio.to_thread(write_string_atomic, self.active_live_file, json_str)

    async def telegram_worker(self):
        while True:
            payload = await self.notification_queue.get()
            api_endpoint = "sendMessage" if 'text' in payload else "sendPhoto"
            api_url = f"https://api.telegram.org/bot{self.telegram_token}/{api_endpoint}"
            retries = 0
            max_retries = 3
            try:
                while retries < max_retries:
                    try:
                        async with self.session.post(api_url, json=payload, timeout=15) as resp:
                            if resp.status == 429:
                                retry_after = int(resp.headers.get("Retry-After", 5))
                                logging.warning(f"Telegram 429 Rate Limit. Sleeping {retry_after}s. (Attempt {retries+1}/{max_retries})")
                                await asyncio.sleep(retry_after)
                                retries += 1
                                continue
                            if resp.status >= 500:
                                logging.warning(f"Telegram 5xx Error. Retrying in 5s. (Attempt {retries+1}/{max_retries})")
                                await asyncio.sleep(5)
                                retries += 1
                                continue
                            resp.raise_for_status()
                            break
                    except asyncio.TimeoutError:
                        logging.warning(f"Telegram timeout. Retrying in 5s. (Attempt {retries+1}/{max_retries})")
                        await asyncio.sleep(5)
                        retries += 1
                    except aiohttp.ClientError as e:
                        logging.error(f"Telegram ClientError: {e}")
                        break
                if retries >= max_retries:
                    logging.error("Telegram payload failed max retries. Re-queueing with penalty.")
                    await asyncio.sleep(30)
                    await self.notification_queue.put(payload)
            except Exception as e:
                logging.error(f"Telegram Worker Error: {e}")
            finally:
                self.notification_queue.task_done()
                await asyncio.sleep(1.5)

    async def ensure_twitch_token(self):
        if not self.twitch_client_id or not self.twitch_client_secret:
            return False

        if not self.twitch_access_token or time.time() >= self.twitch_token_expiry:
            async with self.twitch_auth_lock:
                if not self.twitch_access_token or time.time() >= self.twitch_token_expiry:
                    auth_url = "https://id.twitch.tv/oauth2/token"
                    payload = {
                        'client_id': self.twitch_client_id,
                        'client_secret': self.twitch_client_secret,
                        'grant_type': 'client_credentials'
                    }
                    try:
                        async with self.session.post(auth_url, data=payload, timeout=10) as resp:
                            resp.raise_for_status()
                            auth_data = await resp.json()
                            self.twitch_access_token = auth_data.get('access_token')
                            if not self.twitch_access_token: return False
                            expires_in = auth_data.get('expires_in', 3600)
                            self.twitch_token_expiry = time.time() + expires_in - 60
                    except Exception as e:
                        logging.error(f"Twitch Auth Error: {e}")
                        return False
        return True

    async def get_twitch_stream_info(self, username):
        now = time.time()
        if username in self.twitch_cache and now < self.twitch_cache[username][2]:
            return self.twitch_cache[username][0], self.twitch_cache[username][1]

        if await self.ensure_twitch_token():
            headers = {'Client-ID': self.twitch_client_id, 'Authorization': f"Bearer {self.twitch_access_token}"}
            stream_url = f"https://api.twitch.tv/helix/streams?user_login={username}"
            try:
                async with self.session.get(stream_url, headers=headers, timeout=10) as resp:
                    resp.raise_for_status()
                    data = (await resp.json()).get('data', [])
                    if data: 
                        game_name = data[0].get('game_name', "")
                        title = data[0].get('title', "")
                        self.twitch_cache[username] = (game_name, title, now + 300)
                        return game_name, title
            except Exception as e:
                logging.error(f"Twitch API Error: {e}")
        return "", ""

    async def queue_notification(self, data, prefix, channel_name):
        video_id = data.get('id')
        raw_title = data.get('title', 'Unknown Title')
        raw_title = re.sub(r'\s\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$', '', raw_title)
        
        url = data.get('webpage_url', f"https://www.youtube.com/watch?v={video_id}")    
        if 'twitch.tv' in url.lower():
            match = re.search(r'twitch\.tv/([^/?]+)', url.lower())
            if match:
                username = match.group(1)
                game_name, twitch_title = await self.get_twitch_stream_info(username)
                if twitch_title: raw_title = twitch_title
                if game_name: raw_title = f"{raw_title} — {game_name}"

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
                time_str += f" — Ended: {end_time_dt.strftime('%H:%M')}"
            else:
                time_str += f" — Ended: {end_time_dt.strftime('%m-%d %H:%M')}"

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
            'chat_id': self.telegram_chat_id,
            'photo': thumb,
            'caption': '\n'.join(caption_parts),
            'parse_mode': 'HTML'
        }
        
        logging.info(f"Notification queued: {raw_title}")
        await self.notification_queue.put(payload)

    async def fetch_latest_items(self, target_url, m_type=''):
        items_range = '1-3' if m_type == 'streams' else ('1' if m_type in ['live', 'targeted'] else '1-2')
        cmd = ['yt-dlp', '-j', '--no-warnings', '--playlist-items', items_range, '--ignore-no-formats-error']
        
        if self.cookies_file and os.path.exists(self.cookies_file) and self.cookies_valid:
            cmd.extend(['--cookies', self.cookies_file])
            
        cmd.append(target_url)
        
        items = []
        process = None
        async with self.scan_semaphore:
            try:
                process = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
                
                if stdout:
                    for line in stdout.decode().strip().split('\n'):
                        if line:
                            try:
                                items.append(json.loads(line))
                            except json.JSONDecodeError:
                                logging.warning(f"Malformed JSON from yt-dlp: {line[:50]}")
                        
                if process.returncode != 0 and stderr:
                    error_msg = stderr.decode().strip()
                    if "Sign in to confirm" in error_msg or "cookies" in error_msg.lower():
                        if self.cookies_valid:
                            self.cookies_valid = False
                            logging.error(f"CRITICAL: Cookie expired for {target_url}. Disabling cookie usage.")
                            await self.notification_queue.put({
                                'chat_id': self.telegram_admin_id,
                                'text': '⚠️ <b>CRITICAL: YouTube cookies expired.</b>\nManual replacement of cookies.txt required to resume restricted scanning.',
                                'parse_mode': 'HTML'
                            })
            except asyncio.TimeoutError:
                logging.warning(f"Timeout fetching data for {target_url}")
            except Exception as e:
                logging.error(f"Async execution error for {target_url}: {e}")
            finally:
                if process and process.returncode is None:
                    try:
                        process.kill()
                        await process.wait()
                    except ProcessLookupError:
                        pass
                    except Exception as e:
                        logging.error(f"Process kill error: {e}")
                    
        return items

    def check_keywords(self, text, keywords):
        if not keywords: return True
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in keywords)

    async def process_channel(self, channel, allowed_tab):
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
            
        items = await self.fetch_latest_items(target_url, m_type=allowed_tab)
        
        for item in items:
            v_id = item.get('id')
            if not v_id or v_id == self.free_chat_id: continue
            if not self.check_keywords(f"{item.get('title', '')} {item.get('description', '')}", keywords): continue

            live_status = item.get('live_status')
            
            if live_status == 'is_upcoming':
                should_notify = False
                if f"{v_id}_scheduled" not in self.seen_ids:
                    self._mark_seen(v_id, "_scheduled")
                    state_modified = True
                    should_notify = True
                        
                if should_notify:
                    prefix = "SCHEDULED - PREMIERE" if allowed_tab == 'videos' else ("SCHEDULED - Collab" if keywords else f"SCHEDULED - {'Twitch' if is_twitch else 'Youtube'}")
                    await self.queue_notification(item, prefix, channel_name)
                    
                # DECOUPLED LOGIC: 
                # This must run every time an is_upcoming stream is detected, regardless of notification status.
                raw_sched = item.get('scheduled_timestamp') or item.get('release_timestamp')
                
                if raw_sched:
                    if v_id not in self.scheduled_streams:
                        self.scheduled_streams[v_id] = {
                            'id': v_id,
                            'url': item.get('webpage_url', f"https://www.youtube.com/watch?v={v_id}"),
                            'timestamp': float(raw_sched),
                            'channel_name': channel_name,
                            'is_collab': bool(keywords),
                            'is_twitch': is_twitch,
                            'is_premiere': allowed_tab == 'videos',
                            'added_at': time.time()
                        }
                        await self.save_scheduled()
            
            elif live_status == 'is_live':
                if is_twitch and '/videos/' in item.get('webpage_url', '').lower(): continue

                should_notify = False
                state_marker = f"{v_id}_live"
                if state_marker not in self.seen_ids:
                    self._mark_seen(v_id, "_live")
                    self.seen_ids.pop(f"{v_id}_scheduled", None)
                    state_modified = True
                    should_notify = True
                        
                if should_notify:
                    rich_item = item
                    if not is_twitch:
                        watch_url = f"https://www.youtube.com/watch?v={v_id}"
                        try:
                            deep_data = await self.fetch_latest_items(watch_url, m_type='live')
                            if deep_data: rich_item.update(deep_data[0])
                        except Exception as e:
                            logging.warning(f"Deep extraction failed, using shallow data: {e}")

                    prefix = "PREMIERE - " + ("Collab" if keywords else "Youtube") if allowed_tab == 'videos' else ("LIVE - Collab" if keywords else f"LIVE - {'Twitch' if is_twitch else 'Youtube'}")
                    await self.queue_notification(rich_item, prefix, channel_name)
                    
                # DECOUPLED LOGIC: State management must occur regardless of notification status.
                if v_id in self.scheduled_streams:
                    self.scheduled_streams.pop(v_id, None)
                    await self.save_scheduled()
                    
                if v_id not in self.active_lives:
                    self.active_lives[v_id] = {
                        'id': v_id,
                        'url': f"https://www.youtube.com/watch?v={v_id}" if not is_twitch else base_url,
                        'timestamp': time.time(),
                        'channel_name': channel_name,
                        'is_twitch': is_twitch
                    }
                    await self.save_active_lives()
            
            else:
                should_notify = False
                prefix_label = ""
                
                if allowed_tab in ['videos', 'shorts']:
                    if f"{v_id}_live" in self.seen_ids and allowed_tab == 'videos':
                        if v_id not in self.seen_ids:
                            self._mark_seen(v_id)
                            self.seen_ids.pop(f"{v_id}_live", None)
                            state_modified = True
                            should_notify = True
                            prefix_label = "VIDEO UPLOAD"
                    elif not any(f"{v_id}{suffix}" in self.seen_ids for suffix in ['', '_scheduled', '_live', '_vod']):
                        self._mark_seen(v_id)
                        state_modified = True
                        should_notify = True
                        prefix_label = "NEW SHORTS" if allowed_tab == 'shorts' else "VIDEO UPLOAD"
                else:
                    state_marker = f"{v_id}_vod"
                    if state_marker not in self.seen_ids:
                        self._mark_seen(v_id, "_vod")
                        self.seen_ids.pop(f"{v_id}_scheduled", None)
                        self.seen_ids.pop(f"{v_id}_live", None)
                        state_modified = True
                        should_notify = True
                        prefix_label = "VOD ARCHIVE"
                
                if should_notify:
                    await self.queue_notification(item, prefix_label, channel_name)
                    
                # DECOUPLED LOGIC: 
                # If an item is definitively a VOD, Short, or Video, it cannot be scheduled or active.
                if v_id in self.scheduled_streams:
                    self.scheduled_streams.pop(v_id, None)
                    await self.save_scheduled()
                    
                if v_id in self.active_lives:
                    self.active_lives.pop(v_id, None)
                    await self.save_active_lives()

        if state_modified:
            await self.save_state()

    async def fast_block_worker(self, channels, interval):
        STALE_SCHEDULE_THRESHOLD_SEC = 3600
        TARGETED_POLL_PRE_START_SEC = 300
        MAX_SCHEDULE_RETENTION_SEC = 86400 * 7 # Fallback retention: 7 days
        
        yt_main_live = [c for c in channels if not c.get('keywords') and 'live' in c.get('monitor', [])]
        queue_idx = 0
        
        while True:
            start_time = time.time()
            try:
                current_time = time.time()
                
                stale_keys = [
                    k for k, s in self.scheduled_streams.items() 
                    if (current_time > s['timestamp'] + STALE_SCHEDULE_THRESHOLD_SEC) or 
                       (current_time > s.get('added_at', current_time) + MAX_SCHEDULE_RETENTION_SEC)
                ]
                if stale_keys:
                    for k in stale_keys:
                        self.scheduled_streams.pop(k, None)
                    await self.save_scheduled()
                
                dynamic_queue = []
                for c in yt_main_live:
                    dynamic_queue.append({'type': 'channel', 'data': c})
                    
                for s in self.scheduled_streams.values():
                    if current_time >= s['timestamp'] - TARGETED_POLL_PRE_START_SEC:
                        dynamic_queue.append({'type': 'scheduled', 'data': s})
                            
                if not dynamic_queue:
                    elapsed = time.time() - start_time
                    await asyncio.sleep(max(5.0, interval - elapsed))
                    continue
                    
                if queue_idx >= len(dynamic_queue):
                    queue_idx = 0
                    
                current_item = dynamic_queue[queue_idx]
                
                if current_item['type'] == 'scheduled':
                    stream = current_item['data']
                    v_id = stream['id']
                    logging.info(f"Fast Scan (scheduled): '{stream.get('channel_name')}'")
                    
                    items = await self.fetch_latest_items(stream['url'], m_type='targeted')
                    if items and items[0].get('live_status') == 'is_live':
                        needs_state_save = False
                        should_notify_targeted = False
                        
                        if f"{v_id}_live" not in self.seen_ids:
                            self._mark_seen(v_id, "_live")
                            self.seen_ids.pop(f"{v_id}_scheduled", None)
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
                                
                            await self.queue_notification(items[0], prefix_label, stream['channel_name'])
                                
                        if needs_state_save:
                            await self.save_state()
                            
                        self.scheduled_streams.pop(v_id, None)
                        await self.save_scheduled()
                            
                        if v_id not in self.active_lives:
                            self.active_lives[v_id] = {
                                'id': v_id,
                                'url': stream['url'],
                                'timestamp': time.time(),
                                'channel_name': stream['channel_name'],
                                'is_twitch': stream.get('is_twitch', False)
                            }
                            await self.save_active_lives()
                                
                elif current_item['type'] == 'channel':
                    target_channel = current_item['data']
                    logging.info(f"Fast Scan (live): '{target_channel.get('name')}'")
                    await self.process_channel(target_channel, 'live')

            except Exception as e:
                logging.error(f"Fast Block Error: {e}")
                
            queue_idx += 1
            elapsed = time.time() - start_time
            await asyncio.sleep(max(5.0, interval - elapsed))

    async def rolling_queue_worker(self, queue_name, channels, tabs, interval):
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
            start_time = time.time()
            try:
                dynamic_queue = list(static_queue)
                
                if queue_name == "Main":
                    for s in list(self.active_lives.values()):
                        dynamic_queue.append({'type': 'active_live', 'data': s})
                        
                if queue_idx >= len(dynamic_queue):
                    queue_idx = 0
                    
                current_item = dynamic_queue[queue_idx]
                
                if isinstance(current_item, dict) and current_item.get('type') == 'active_live':
                    stream = current_item['data']
                    v_id = stream['id']
                    logging.info(f"{queue_name} Scan (active_live): '{stream.get('channel_name')}'")
                    
                    if time.time() > stream['timestamp'] + 3600:
                        self.active_lives.pop(v_id, None)
                        await self.save_active_lives()
                    else:
                        items = await self.fetch_latest_items(stream['url'], m_type='targeted')
                        if items:
                            live_status = items[0].get('live_status')
                            if live_status == 'is_live':
                                stream['timestamp'] = time.time()
                                stream['failures'] = 0
                            elif live_status in ['was_live', 'post_live'] or (live_status is None and items[0].get('duration')):
                                needs_state_save = False
                                should_notify_vod = False
                                if f"{v_id}_vod" not in self.seen_ids:
                                    self._mark_seen(v_id, "_vod")
                                    self.seen_ids.pop(f"{v_id}_live", None)
                                    needs_state_save = True
                                    should_notify_vod = True
                                if should_notify_vod:
                                    await self.queue_notification(items[0], "VOD ARCHIVE", stream['channel_name'])
                                if needs_state_save:
                                    await self.save_state()
                                    
                                self.active_lives.pop(v_id, None)
                                await self.save_active_lives()
                        else:
                            stream['failures'] = stream.get('failures', 0) + 1
                            if stream['failures'] >= 3:
                                self.active_lives.pop(v_id, None)
                                await self.save_active_lives()
                else:
                    target_channel, current_tab = current_item
                    logging.info(f"{queue_name} Scan ({current_tab}): '{target_channel.get('name')}'")
                    await self.process_channel(target_channel, current_tab)
                    
            except Exception as e:
                logging.error(f"{queue_name} Worker Error: {e}")
            
            queue_idx += 1
            elapsed = time.time() - start_time
            await asyncio.sleep(max(5.0, interval - elapsed))

    async def run(self):
        channels = self.config.get('CHANNELS', [])
        live_interval = self.config.get('LIVE_CHECK_INTERVAL', 60)
        collab_live_interval = self.config.get('COLLAB_LIVE_INTERVAL', 65)
        main_interval = self.config.get('MAIN_SCAN_INTERVAL', 60)
        collab_interval = self.config.get('COLLAB_SCAN_INTERVAL', 60)
        
        yt_main = [c for c in channels if not c.get('keywords')]
        yt_collab = [c for c in channels if c.get('keywords')]

        logging.info(f"Async Monitor active. Main Live: {live_interval}s | Collab Live: {collab_live_interval}s | Main VOD: {main_interval}s | Collab VOD: {collab_interval}s")

        async with aiohttp.ClientSession() as session:
            self.session = session
            tasks = [
                asyncio.create_task(self.telegram_worker()),
                # Tier 1 & 2: Fast Targeted Polls & Main Live
                asyncio.create_task(self.fast_block_worker(channels, live_interval)),
                # Tier 3: Main VODs
                asyncio.create_task(self.rolling_queue_worker("Main", yt_main, ['streams', 'videos', 'shorts'], main_interval)),
                # Tier 4a: Collab Live (Independent Interval)
                asyncio.create_task(self.rolling_queue_worker("Collab Live", yt_collab, ['live'], collab_live_interval)),
                # Tier 4b: Collab VODs
                asyncio.create_task(self.rolling_queue_worker("Collab VOD", yt_collab, ['streams', 'videos', 'shorts'], collab_interval))
            ]
            await asyncio.gather(*tasks)

async def main():
    config = load_json(CONFIG_FILE, {})
    if not config:
        logging.error("Invalid or missing config.json")
        return
    
    monitor = YTChannelMonitor(config)
    await monitor.run()

if __name__ == '__main__':
    asyncio.run(main())
