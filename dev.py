import argparse
import logging
import sys, os
import urllib.error
import urllib.parse
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup
from datetime import datetime

import asyncio
import aiofiles
import aiohttp
from aiohttp import ClientSession, TCPConnector
import aiosqlite

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True

async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    semaphore = kwargs['semaphore']
    async with semaphore:
        resp = await session.request(method="GET", url=url)
        resp.raise_for_status()
        logger.debug("Got response [%s] for URL: %s", resp.status, unquote(url))
        html = await resp.text()
        return html

async def parse(url: str, session: ClientSession, **kwargs) -> set:
    files = []
    directories = []
    try:
        html = await fetch_html(url=url, session=session, **kwargs)
    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            unquote(url),
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return files, directories
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return files, directories
    else:
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            href = link.get('href')
            if href != '../' and not href.endswith('/'):
                try:
                    abslink = urljoin(url, href)
                except (urllib.error.URLError, ValueError):
                    logger.exception("Error parsing URL: %s", unquote(link))
                pass
                filename = unquote((urlparse(abslink).path))
                timestamp_str = link.next_sibling.strip().split()[0:2]
                timestamp = datetime.strptime(' '.join(timestamp_str), '%d-%b-%Y %H:%M')
                timestamp_unix = int(timestamp.timestamp())
                filesize = link.next_sibling.strip().split()[2]
                files.append((abslink, filename, timestamp_unix, filesize))
            elif href != '../':
                directories.append(urljoin(url, href))
        return files, directories


async def write_one(database, url: str, db, **kwargs) -> list:
    files, directories = await parse(url=url, **kwargs)
    if not files:
        return directories
    await db.executemany('INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)', files)
    await db.commit()
    logger.debug("Wrote results for source URL: %s", unquote(url))
    return directories


async def bulk_crawl_and_write(database, url: str, session, db, **kwargs) -> None:
    if session.closed or not session:
        logger.info("session is closed")
        session = await ClientSession(connector=TCPConnector(ssl=False, limit=0, ttl_dns_cache=600))
        asyncio.sleep(1)
    if not db:
        db = await aiosqlite.connect(database)
    tasks = []
    directories = await write_one(database=database, url=url, session=session, db=db, **kwargs)
    for url in directories:
        task = asyncio.create_task(bulk_crawl_and_write(database=database, url=url, session=session, db=db, **kwargs))
        tasks.append(task)
    await asyncio.gather(*tasks)


async def process_temp_database(temp_db_file, db_file, media_path, args):
    try:
        if not args.no_download:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                temp_conn = sqlite3.connect(temp_db_file)
                temp_c = temp_conn.cursor()
                temp_c.execute('SELECT COUNT(*) FROM files')
                total_records = temp_c.fetchone()[0]
                chunk_size = 1000  # Adjust the chunk size as needed
                offset = 0
                while offset < total_records:
                    temp_c.execute('SELECT * FROM files LIMIT ? OFFSET ?', (chunk_size, offset))
                    temp_files = temp_c.fetchall()
                    await store_in_database(db_file, temp_files, media_path, session)
                    offset += chunk_size
        else:
            print("Skipping download.")
    finally:
        temp_conn.close()



async def main() :
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", type=str, default=os.path.join(os.path.dirname(__file__), "media"), help="Path to store downloaded media files")
    parser.add_argument("--count", type=int, default=100, help="Max concurrent HTTP Requests")
    parser.add_argument("--debug", default=False, help="Verbose debug")
    parser.add_argument("--url", type=str, default="https://emby.xiaoya.pro/", help="Verbose debug")
    args = parser.parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    url = args.url
    database = "file.db"
    semaphore = asyncio.Semaphore(args.count)
    async with aiosqlite.connect(database) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS files
                         (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER, filesize INTERGER)''')
        async with ClientSession(connector=TCPConnector(ssl=False, limit=0, ttl_dns_cache=600)) as session:
            await bulk_crawl_and_write(database=database, url=url, session=session, db=db, semaphore=semaphore)

if __name__ == "__main__":
    assert sys.version_info >= (3, 10), "Script requires Python 3.10+."
    asyncio.run(main())