[![AI Generated](https://img.shields.io/badge/AI_Generated-Gemini-blue.svg)](https://gemini.google.com)
## Acknowledgments

* **Code Generation:** The core logic and boilerplate for this project were generated using Gemini. All AI-generated code was subsequently reviewed and tested.
# yt-channel-monitor

An asynchronous Python monitor that tracks YouTube and Twitch channels for specific keywords, live streams, premieres, and VOD uploads. It uses `yt-dlp` for YouTube data extraction and the official Twitch API for Twitch streams, sending real-time formatting alerts to a designated Telegram chat.

## Features

* **Multi-Platform:** Monitors both YouTube and Twitch.
* **Asynchronous Processing:** Utilizes `asyncio` to run independent, non-blocking polling queues:
  * **Fast Block:** High-frequency polling for scheduled streams nearing their start time.
  * **Main Queue:** Scans primary channels for all uploads and streams.
  * **Collab Queue:** Scans secondary channels, filtering strictly for specific keywords.
* **State Management:** Tracks video IDs locally (`seen_ids.json` and `scheduled.json`) to prevent duplicate notifications.
* **Telegram Integration:** Sends formatted messages with video thumbnails, channel names, timestamps, and direct URLs.

## Requirements

* Python 3.10 or higher
* [yt-dlp](https://github.com/yt-dlp/yt-dlp) installed and accessible in your system PATH.
* A valid Telegram Bot Token and Chat ID.
* Twitch API Client ID and Secret.
* YouTube cookies file (required for bypassing age-restrictions or rate limits).

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/shmilyhua/yt-channel-monitor.git
   cd yt-channel-monitor
   ```

2. This project uses `uv` for dependency management. Install the dependencies:
   ```bash
   uv venv --python 3.13
   uv sync
   ```
   *(Alternatively, you can install the dependencies listed in `pyproject.toml` via pip).*

## Configuration

1. Create a `config.json` file in the root directory. This file is ignored by git to protect your credentials.
2. Structure your `config.json` as follows:

```json
{
    "TELEGRAM_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID": "YOUR_TARGET_CHAT_ID",
    "TWITCH_CLIENT_ID": "YOUR_TWITCH_CLIENT_ID",
    "TWITCH_CLIENT_SECRET": "YOUR_TWITCH_CLIENT_SECRET",
    "COOKIES_FILE": "/path/to/your/www.youtube.com_cookies.txt",
    
    "LIVE_CHECK_INTERVAL": 65,
    "MAIN_SCAN_INTERVAL": 129,
    "COLLAB_SCAN_INTERVAL": 121,
    
    "CHANNELS": [
        {
            "name": "神楽めあ / KaguraMea",
            "url": "https://www.youtube.com/@KaguraMea",
            "monitor": ["live", "streams", "videos", "shorts"]
        },
        {
            "name": "さぶめあちゃんねるっ!",
            "url": "https://www.youtube.com/@KaguraMeaNyan",
            "monitor": ["videos"]
        },
        {
            "name": "汚い方の神楽めあ【公式】",
            "url": "https://www.youtube.com/@めあ汚い",
            "monitor": ["videos", "shorts"]
        },
        {
            "name": "神楽めあ",
            "url": "https://www.twitch.tv/kagura0mea",
            "monitor": ["live", "streams"]
        },
        {
            "name": "Tamaki Ch. 犬山たまき / 佃煮のりお",
            "url": "https://www.youtube.com/@犬山たまき佃煮のりお",
            "monitor": ["live", "streams", "videos", "shorts"],
            "keywords": ["神楽めあ", "KaguraMea"]
        }
    ]
}
```

### Channel Routing & Keywords Logic

The presence or absence of the `keywords` key dictates how a channel is processed:

* **Main Channels (No `keywords`):** Channels configured without the `keywords` array are routed to the **Main Queue**. The script will trigger alerts for *every* new stream or upload found on these channels. The polling frequency is governed by `MAIN_SCAN_INTERVAL`.
* **Collab Channels (Has `keywords`):** Channels configured with the `keywords` array are routed to the **Collab Queue**. The script will only trigger alerts if the stream/video title or description matches one of the provided keywords. The polling frequency is governed by `COLLAB_SCAN_INTERVAL`.

### Twitch Channel Configuration

To monitor a Twitch channel, use the standard Twitch URL format. The application automatically detects `twitch.tv` in the URL and routes requests appropriately. 

```json
{
    "name": "Twitch Streamer Name",
    "url": "[https://www.twitch.tv/username](https://www.twitch.tv/username)",
    "monitor": ["live", "streams"]
}
```

* **live:** Monitors the channel for active live broadcasts.
* **streams:** Monitors the channel's VOD archives.
* **Note:** Twitch channels do not support the `shorts` tab. Including it in the `monitor` array for a Twitch channel will be safely ignored by the script to save network requests.

## Usage

Start the monitor by running:

```bash
uv run monitor.py
```

The application will generate `seen_ids.json`, `scheduled.json`, and `monitor.log` locally to track execution state and logging output.
