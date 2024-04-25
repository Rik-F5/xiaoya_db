
import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup
from datetime import datetime

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

HREF_RE = re.compile(r'href="(.*?)"')

async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.
    """

    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status, unquote(url))
    html = await resp.text()
    return html

async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""
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
        logger.debug("URL: %s", url)
        for link in soup.find_all('a'):
            logger.debug("Find link: %s", link)
            href = link.get('href')
            logger.debug("Find href: %s", href)
            if href != '../' and not href.endswith('/'):
                try:
                    abslink = urljoin(url, href)
                    logger.debug("Find URL: %s", abslink)
                except (urllib.error.URLError, ValueError):
                    logger.exception("Error parsing URL: %s", unquote(link))
                pass
                filename = unquote((urlparse(abslink).path))
                timestamp_str = link.next_sibling.strip().split()[0:2]
                timestamp = datetime.strptime(' '.join(timestamp_str), '%d-%b-%Y %H:%M')
                timestamp_unix = int(timestamp.timestamp())
                filesize = link.next_sibling.strip().split()[2]
                files.append((abslink, filename, timestamp_unix, filesize))
                logger.debug("Found %d files for %s", len(files), unquote(url))
            elif href != '../':
                directories.append(urljoin(url, href))
        return files, directories


async def write_one(database: IO, url: str, **kwargs) -> list:
    """Write the found HREFs from `url` to `file`."""
    files, directories = await parse(url=url, **kwargs)
    if not files:
        return directories
    logger.debug(files)
    #async with aiosqlite.connect(database) as db:
    #    await db.execute('''CREATE TABLE IF NOT EXISTS files
    #                     (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER, filesize INTERGER)''')
    #    await db.executemany('INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)', files)
    #    await db.commit()
    #    logger.info("Wrote results for source URL: %s", unquote(url))
    return directories

async def bulk_crawl_and_write(database: IO, url: str, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        directories = await write_one(database=database, url=url, session=session, **kwargs)
        for url in directories:
            task = asyncio.create_task(bulk_crawl_and_write(database=database, url=url, **kwargs))
            tasks.append(task)
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent

    url = "https://emby.xiaoya.pro/"

    database = "file.db"

    asyncio.run(bulk_crawl_and_write(database=database, url=url))