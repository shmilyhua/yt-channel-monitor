# yt-channel-monitor

An asynchronous Python monitor that tracks YouTube and Twitch channels for specific keywords, live streams, premieres, and VOD uploads. It uses `yt-dlp` for YouTube data extraction and the official Twitch API for Twitch streams, sending real-time formatting alerts to a designated Telegram chat.

## Features

* **Multi-Platform:** Monitors both YouTube and Twitch.
* **Asynchronous Processing:** Utilizes `asyncio` to run independent, non-blocking polling queues (Fast block for scheduled streams, Main queue, and Collab queue).
* **State Management:** Tracks video IDs locally (`seen_ids.json` and `scheduled.json`) to prevent duplicate notifications.
* **Keyword Filtering:** Only triggers notifications if stream titles or descriptions contain specified keywords (e.g., tracking specific VTuber collaborations across multiple channels).
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
   git clone [https://github.com/yourusername/yt-channel-monitor.git](https://github.com/yourusername/yt-channel-monitor.git)
   cd yt-channel-monitor
   ```

2. This project uses `uv` for dependency management. Install the dependencies:
   ```bash
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
            "name": "Target Channel Name",
            "url": "[https://www.youtube.com/@TargetChannel](https://www.youtube.com/@TargetChannel)",
            "monitor": ["live", "streams", "videos", "shorts"],
            "keywords": ["Keyword1", "Keyword2"] 
        }
    ]
}
```
*Note: Omit the `keywords` array if you want to track all uploads/streams from a channel.*

## Usage

Start the monitor by running:

```bash
python monitor.py
```

The application will generate `seen_ids.json`, `scheduled.json`, and `monitor.log` locally to track execution state and logging output.

## License

[MIT](LICENSE)
