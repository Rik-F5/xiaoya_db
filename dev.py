
import logging
import sys
from typing import IO
import urllib.error
import urllib.parse
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup
from datetime import datetime
import pathlib


import asyncio
import aiofiles
import aiohttp
from aiohttp import ClientSession, TCPConnector
import aiosqlite

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
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


async def write_one(database: IO, url: str, db, **kwargs) -> list:
    files, directories = await parse(url=url, **kwargs)
    if not files:
        return directories
    await db.execute('''CREATE TABLE IF NOT EXISTS files
                         (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER, filesize INTERGER)''')
    await db.executemany('INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)', files)
    await db.commit()
    logger.debug("Wrote results for source URL: %s", unquote(url))
    return directories

async def bulk_crawl_and_write(database: IO, url: str, session, db, **kwargs) -> None:
    if not session:
        session = ClientSession(connector=TCPConnector(ssl=False, limit=10, ttl_dns_cache=600))
    if not db:
        db = await aiosqlite.connect(database)
    tasks = []
    directories = await write_one(database=database, url=url, session=session, db=db, **kwargs)
    for url in directories:
        task = asyncio.create_task(bulk_crawl_and_write(database=database, url=url, session=session, db=db, **kwargs))
        tasks.append(task)
    await asyncio.gather(*tasks)


async def main() :
    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    url = "https://emby.xiaoya.pro/"
    database = "file.db"
    db = await aiosqlite.connect(database)
    semaphore = asyncio.Semaphore(100)
    async with ClientSession(connector=TCPConnector(ssl=False, limit=0, ttl_dns_cache=600)) as session:
        await bulk_crawl_and_write(database=database, url=url, session=session, db=db, semaphore=semaphore)
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())