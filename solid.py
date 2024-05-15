import argparse
import logging
import sys, os
import urllib.error
import urllib.parse
import urllib.request
from urllib.parse import urljoin, urlparse, unquote, quote
import aiohttp.client_exceptions
from bs4 import BeautifulSoup
from datetime import datetime
import random
import re
import gzip

import asyncio
import aiofiles
import aiohttp
from aiohttp import ClientSession, TCPConnector
import aiosqlite
import aiofiles.os as aio_os



logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("emd")
logging.getLogger("chardet.charsetprober").disabled = True


s_paths_all = [
    quote('PikPak/'),
    quote('动漫/'),
    quote('每日更新/'),
    quote('电影/'),
    quote('电视剧/'),
    quote('纪录片/'),
    quote('纪录片（已刮削）/'),
    quote('综艺/'),
    quote('音乐/'),
    quote('📺画质演示测试（4K，8K，HDR，Dolby）/')
]



s_paths = [
    quote('每日更新/'),
    quote('电影/2023/'),
    quote('纪录片（已刮削）/'),
    quote('音乐/')
]

s_pool = [
    "https://emby.xiaoya.pro/",
    "https://icyou.eu.org/",
    "https://lanyuewan.cn/"
]

s_folder = [
    ".sync"
]

s_ext = [
    ".ass",
    ".srt",
    ".ssa"
]

# CF blocks urllib...

custom_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
opener = urllib.request.build_opener()
opener.addheaders = [('User-Agent', custom_user_agent)]
urllib.request.install_opener(opener)


def pick_a_pool_member(url_list):
    random.shuffle(url_list)
    for member in url_list:
        try:
            logger.debug("Testing: %s", member)
            response = urllib.request.urlopen(member)
            if response.getcode() == 200:
                logger.info("Picked: %s", member)
                return member
        except Exception as e:
            logger.info("Error accessing %s: %s", member, e)
            pass
    return None

def current_amount(url, media, paths):
    listfile = os.path.join(media, ".scan.list.gz")
    try:
        res = urllib.request.urlretrieve(url, listfile)
        with gzip.open(listfile) as response:
            pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2} \/(.*)$'
            hidden_pattern = r'^.*?\/\..*$'
            matching_lines = 0
            for line in response:
                try:
                    line = line.decode(encoding='utf-8').strip()
                    match = re.match(pattern, line)
                    if match:
                        file = match.group(1)
                        if any(file.startswith(unquote(path)) for path in paths):
                            if not re.match(hidden_pattern, file):
                                matching_lines += 1
                except:
                    logger.error("Error decoding line: %s", line)
        return matching_lines
    except urllib.error.URLError as e:
        print("Error:", e)
        return -1

async def fetch_html(url, session, **kwargs) -> str:
    semaphore = kwargs['semaphore']
    async with semaphore:
        async with session.request(method="GET", url=url) as resp:
            logger.debug("Request Headers for [%s]: [%s]", unquote(url), resp.request_info.headers)
            resp.raise_for_status()       
            logger.debug("Response Headers for [%s]: [%s]", unquote(url), resp.headers)
            logger.debug("Got response [%s] for URL: %s", resp.status, unquote(url))
            return await resp.text()

async def parse(url, session, **kwargs) -> set:
    files = []
    directories = []
    try:
        html = await fetch_html(url=url, session=session, **kwargs)
    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
        aiohttp.ClientPayloadError,
        aiohttp.ClientResponseError,
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
            if href != '../' and not href.endswith('/') and href != 'scan.list':
                try:
                    abslink = urljoin(url, href)
                except (urllib.error.URLError, ValueError):
                    logger.exception("Error parsing URL: %s", unquote(link))
                pass
                filename = unquote(urlparse(abslink).path)
                timestamp_str = link.next_sibling.strip().split()[0:2]
#TODO: Need to handle /cdn-cgi/l/email-protection
                try:
                    timestamp = datetime.strptime(' '.join(timestamp_str), '%d-%b-%Y %H:%M')
                except:
                    logger.error("%s: %s", filename, timestamp_str)
                    continue
                timestamp_unix = int(timestamp.timestamp())
                filesize = link.next_sibling.strip().split()[2]
                files.append((abslink, filename, timestamp_unix, filesize))
            elif href != '../':
                directories.append(urljoin(url, href))
            href = None
        soup = None
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
        try: 
            async with session.get(url) as response:
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
                    logger.error("Failed to download: %s [Response code: %s]", filename, response.status)
        except Exception as e:
            logger.exception("Download exception: %s", e)
            


async def download_files(files, session, **kwargs):
    download_tasks = set()
    for file in files:
        if await need_download(file, **kwargs) == True:
            task = asyncio.create_task(download(file, session, **kwargs))
            task.add_done_callback(download_tasks.discard)
            download_tasks.add(task)
    await asyncio.gather(*download_tasks)


async def create_table(conn):
    async with conn.execute('''
        CREATE TABLE IF NOT EXISTS files (
            filename TEXT,
            timestamp INTEGER NULL,
            filesize INTEGER NULL)
    '''):
        pass

async def insert_files(conn, items):
    await conn.executemany('INSERT OR REPLACE INTO files VALUES (?, ?, ?)', items)
    await conn.commit()

async def exam_file(file, media):
    stat = await aio_os.stat(file)
    return file[len(media):], int(stat.st_mtime), stat.st_size

async def process_folder(conn, folder, media):
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if d not in s_folder]
        for file in files:
            items = []
            if not file.startswith('.') and not file.lower().endswith(tuple(s_ext)):
                items.append((os.path.join(root, file)[len(media):], None, None))
                await insert_files(conn, items)

async def generate_localdb(db, media, paths):
    async with aiosqlite.connect(db) as conn:
        await create_table(conn)
        for path in paths:
            logger.info("Processing %s", unquote(os.path.join(media, path)))
            await process_folder(conn, unquote(os.path.join(media, path)), media)
        await conn.close()

async def write_one(url, session, db_session, **kwargs) -> list:
    # This is a hack.. To be compatible with the website with the full data rather than updating ones.
    if urlparse(url).path == '/':
        directories = []
        for path in kwargs['paths']:
            directories.append(urljoin(url, path))
        return directories
    files, directories = await parse(url=url, session=session, **kwargs)
    if not files:
        return directories
    if kwargs['media']:
        await download_files(files=files, session=session, **kwargs)
    if db_session:
        items = []
        for file in files:
            items.append(file[1:])
        await db_session.executemany('INSERT OR REPLACE INTO files VALUES (?, ?, ?)', items)
        await db_session.commit()
        logger.debug("Wrote results for source URL: %s", unquote(url))
    return directories


async def bulk_crawl_and_write(url, session, db_session, depth=0, **kwargs) -> None:
    tasks = set()
    directories = await write_one(url=url, session=session, db_session=db_session, **kwargs)
    for url in directories:
        task = asyncio.create_task(bulk_crawl_and_write(url=url, session=session, db_session=db_session, depth=depth + 1, **kwargs))
        task.add_done_callback(tasks.discard)
        tasks.add(task)
        if depth == 0:
            await asyncio.gather(*tasks)
    await asyncio.gather(*tasks)


async def compare_databases(localdb, tempdb, total_amount):

    async with aiosqlite.connect(localdb) as conn1, aiosqlite.connect(tempdb) as conn2:
        cursor1 = await conn1.cursor()
        cursor2 = await conn2.cursor()

        await cursor1.execute("SELECT filename FROM files")
        local_filenames = set(filename[0] for filename in await cursor1.fetchall())

        await cursor2.execute("SELECT filename FROM files")
        temp_filenames = set(filename[0] for filename in await cursor2.fetchall())
        gap = abs(len(temp_filenames) - total_amount)

        if gap < 10 :
            if not gap == 0: 
                logger.warning("Total amount do not match: %d -> %d. But the gap %d is less than 10, purging anyway...", total_amount, len(temp_filenames), abs(len(temp_filenames) - total_amount))
            diff_filenames = local_filenames - temp_filenames
            return diff_filenames
        else:
            logger.error("Total amount do not match: %d -> %d. Purges are skipped", total_amount, len(temp_filenames))
            return []

    
async def purge_removed_files(localdb, tempdb, media, total_amount):
    for file in await compare_databases(localdb, tempdb, total_amount):
        logger.info("Purged %s", file)
        try:
            os.remove(media + file)
        except Exception as e:
            logger.error("Unable to remove %s due to %s", file, e)


def test_media_folder(media, paths):
    t_paths = [os.path.join(media, unquote(path)) for path in paths]
    if all(os.path.exists(os.path.abspath(path)) for path in t_paths):
        return True
    else:
        return False


async def main() :
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", metavar="<folder>", type=str, default=None, required=True, help="Path to store downloaded media files [Default: %(default)s]")
    parser.add_argument("--count", metavar="[number]", type=int, default=100, help="Max concurrent HTTP Requests [Default: %(default)s]")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, type=bool, default=False, help="Verbose debug [Default: %(default)s]")
    parser.add_argument("--db", action=argparse.BooleanOptionalAction, type=bool, default=False, help="<Python3.12+ required> Save into DB [Default: %(default)s]")
    parser.add_argument("--nfo", action=argparse.BooleanOptionalAction, type=bool, default=False, help="Download NFO [Default: %(default)s]")
    parser.add_argument("--url", metavar="[url]", type=str, default=None, help="Download path [Default: %(default)s]")
    parser.add_argument("--purge", action=argparse.BooleanOptionalAction, type=bool, default=True, help="Purge removed files [Default: %(default)s]")
    parser.add_argument("--all", action=argparse.BooleanOptionalAction, type=bool, default=False, help="Download all folders [Default: %(default)s]")



    args = parser.parse_args()
    if args.debug == True:
        logging.getLogger("emd").setLevel(logging.DEBUG)
    if args.all == True:
        paths = s_paths_all
        s_pool.pop(0)
        if args.purge == True:
            args.db = True
    else:
        paths = s_paths
    if args.media:
        if not test_media_folder(args.media, paths):
            logging.error("The %s doesn't contain the desired folders, please correct the --media parameter", args.media)
            exit()
        else:
            media = args.media.rstrip('/')
    if not args.url:
        url = pick_a_pool_member(s_pool)
    else:
        url = args.url
    if urlparse(url).path != '/' and (args.purge or args.db):
        logger.warning("--db or --purge only support in root path mode")
        exit()
    if not url:
        logger.info("No servers are reachable, please check your Internet connection...")
        exit()
    if urlparse(url).path == '/':
        total_amount = current_amount(url + '.scan.list.gz', media, paths)
        logger.info("There are %d files in %s", total_amount, url)
    semaphore = asyncio.Semaphore(args.count)
    db_session = None
    if args.db or args.purge:
        assert sys.version_info >= (3, 12), "DB function requires Python 3.12+."
        localdb = os.path.join(media, ".localfiles.db")
        tempdb = os.path.join(media, ".tempfiles.db")
        if not os.path.exists(localdb):
            await generate_localdb(localdb, media, paths)
        elif args.db:
            os.remove(localdb)
            await generate_localdb(localdb, media, paths)
        db_session = await aiosqlite.connect(tempdb)
        await create_table(db_session)
    async with ClientSession(connector=TCPConnector(ssl=False, limit=0, ttl_dns_cache=600), timeout=aiohttp.ClientTimeout(total=36000)) as session:
        await bulk_crawl_and_write(url=url, session=session, db_session=db_session, semaphore=semaphore, media=media, nfo=args.nfo, paths=paths)
    if db_session:
        await db_session.commit()
        await db_session.close()
    if args.purge:
        await purge_removed_files(localdb, tempdb, media, total_amount)
        os.remove(localdb)
        os.rename(tempdb, localdb)
    

if __name__ == "__main__":
    assert sys.version_info >= (3, 10), "Script requires Python 3.10+."
    asyncio.run(main())