# Discord Scheduled Messenger

A simple message scheduler for Discord. Enter a message and a time interval, and message will be automatically sent by bot at specified intervals. Front end is accessible via discord commands. Back end connects to an AWS DynamoDB database that will sync messages to APScheduler. Just ~~to annoy friends~~ have some fun.

## Requirements

Python 3.10

## Setup

### Env

Create a .env file containing:

```
DISCORD_TOKEN=''
TABLE_NAME=''
REGION_NAME=''
```

### Prerequisites
```bash
python -m venv .venv
pip install requirements.txt
```

### Run
```bash
python DiscordMessenger.py
```


