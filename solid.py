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

async def fetch_html(url, session, **kwargs) -> str:
    semaphore = kwargs['semaphore']
    async with semaphore:
        resp = await session.request(method="GET", url=url)
        logger.debug("Request Headers for [%s]: [%s]", unquote(url), resp.request_info.headers)
        resp.raise_for_status()       
        logger.debug("Response Headers for [%s]: [%s]", unquote(url), resp.headers)
        logger.debug("Got response [%s] for URL: %s", resp.status, unquote(url))
        html = await resp.text()
        return html

async def parse(url, session, **kwargs) -> set:
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
                filename = unquote(urlparse(abslink).path)
                timestamp_str = link.next_sibling.strip().split()[0:2]
                timestamp = datetime.strptime(' '.join(timestamp_str), '%d-%b-%Y %H:%M')
                timestamp_unix = int(timestamp.timestamp())
                filesize = link.next_sibling.strip().split()[2]
                files.append((abslink, filename, timestamp_unix, filesize))
            elif href != '../':
                directories.append(urljoin(url, href))
        return files, directories

async def need_download(file, **kwargs):
    url, filename, timestamp, filesize = file
    file_path = os.path.join(kwargs['media'], filename.lstrip('/'))
    if not os.path.exists(file_path):
        logger.debug("%s doesn't exists", file_path)
        return True 
    elif file_path.endswith('.nfo'):
        if not kwargs['nfo']:
            return False
        else:
            pass
    else:
        current_filesize = os.path.getsize(file_path)
        current_timestamp = os.path.getmtime(file_path)
        logger.debug("%s has timestamp: %s and size: %s", filename, current_timestamp, current_filesize)
        if int(filesize) == int(current_filesize) and int(timestamp) <= int(current_timestamp):
            return False
    logger.debug("%s has timestamp: %s and size: %s", filename, timestamp, filesize)
    logger.debug("%s has current_timestamp: %s and current_size: %s", filename, current_timestamp, current_filesize)
    return True

async def download(file, session, **kwargs):
    url, filename, timestamp, filesize = file
    semaphore = kwargs['semaphore']
    async with semaphore:
        response = await session.get(url)
        if response.status == 200:
            file_path = os.path.join(kwargs['media'], filename.lstrip('/'))
            os.umask(0)
            os.makedirs(os.path.dirname(file_path), mode=0o777, exist_ok=True)
            async with aiofiles.open(file_path, 'wb') as f:
                logger.debug("Starting to write file: %s", filename)
                await f.write(await response.content.read())
                logger.debug("Finish to write file: %s", filename)
            os.chmod(file_path, 0o777)
            logger.info("Downloaded: %s", filename)
        else:
            logger.info("Failed to download: %s [Response code: %s]", filename, response.status)


async def download_files(files, session, **kwargs):
    download_tasks = []
    for file in files:
        if await need_download(file, **kwargs) == True:
            task = asyncio.create_task(download(file, session, **kwargs))
            download_tasks.append(task)
    await asyncio.gather(*download_tasks)

    


async def write_one(url, session, db_session, **kwargs) -> list:
    files, directories = await parse(url=url, session=session, **kwargs)
    if not files:
        return directories
    if db_session:
        await db_session.executemany('INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)', files)
        await db_session.commit()
        logger.debug("Wrote results for source URL: %s", unquote(url))
    if kwargs['media']:
        await download_files(files=files, session=session, **kwargs)
    return directories


async def bulk_crawl_and_write(url, session, db_session, **kwargs) -> None:
    tasks = []
    directories = await write_one(url=url, session=session, db_session=db_session, **kwargs)
    for url in directories:
        task = asyncio.create_task(bulk_crawl_and_write(url=url, session=session, db_session=db_session, **kwargs))
        tasks.append(task)
    await asyncio.gather(*tasks)


async def main() :
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", metavar="<folder>", type=str, default=None, help="Path to store downloaded media files [Default: %(default)s]")
    parser.add_argument("--count", metavar="[number]", type=int, default=100, help="Max concurrent HTTP Requests [Default: %(default)s]")
    parser.add_argument("--debug", metavar="[True|False]", type=bool, default=False, help="Verbose debug [Default: %(default)s]")
    parser.add_argument("--db", metavar="[True|False]", type=bool, default=False, help="<Python3.12+ required> Save into DB [Default: %(default)s]")
    parser.add_argument("--nfo", metavar="[True|False]", type=bool, default=False, help="Download NFO [Default: %(default)s]")
    parser.add_argument("--url", metavar="[url]", type=str, default="https://emby.xiaoya.pro/", help="Download path [Default: %(default)s]")
    
    args = parser.parse_args()
    if args.debug:
        logging.getLogger("areq").setLevel(logging.DEBUG)
    url = args.url
    database = "file.db"
    semaphore = asyncio.Semaphore(args.count)
    db_session = None
    if args.db:
        assert sys.version_info >= (3, 12), "DB function requires Python 3.12+."
        db_session = await aiosqlite.connect(database)
        await db_session.execute('''CREATE TABLE IF NOT EXISTS files
                         (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER, filesize INTERGER)''')
    async with ClientSession(connector=TCPConnector(ssl=False, limit=0, ttl_dns_cache=600)) as session:
        await bulk_crawl_and_write(url=url, session=session, db_session=db_session, semaphore=semaphore, media=args.media, nfo=args.nfo)
    if db_session:
        await db_session.commit()
        await db_session.close()
    

if __name__ == "__main__":
    assert sys.version_info >= (3, 10), "Script requires Python 3.10+."
    asyncio.run(main())