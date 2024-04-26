git clone

cd xiaoya_db

python3 -m venv .venv

source .venv/bin/activate

python -m pip install -r requirements.txt

python dev.py --media <Media folder> (Only sync the files)

python dev.py --db   (Only scrap the website and save to DB)

python dev.py --db true --media <Media folder> (Sync files and save to DB)

python dev.py --count <int> concurrent HTTP requests. Default is 100, CF will enforce the concurrent for a single source IP with 503/520. Reduce it if you encounter download failures.

Linux kernel tweak:
ulimit -n 30000

python 3.12+ is needed to avoid an asyncio performance bug

