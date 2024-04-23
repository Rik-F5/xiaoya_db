import argparse
import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import sqlite3
from queue import Queue
from threading import Lock
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote
import os



# Connection pool parameters
POOL_SIZE = 10

# Initialize connection pool
connection_pool = Queue(maxsize=POOL_SIZE)
connection_lock = Lock()

def initialize_connection_pool(temp_db_file):
    for _ in range(POOL_SIZE):
        connection = sqlite3.connect(temp_db_file)
        connection.execute('''CREATE TABLE IF NOT EXISTS files
                     (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER)''')
        connection_pool.put(connection)

# Function to get a connection from the pool
def get_connection():
    with connection_lock:
        return connection_pool.get()

# Function to return a connection to the pool
def return_connection(connection):
    with connection_lock:
        connection_pool.put(connection)


async def fetch_directory_listing(url, session):
    async with session.get(url) as response:
        try:
            return await response.text()
        except UnicodeDecodeError:
            print(unquote(url))
        return await response.text(errors='replace')



async def parse_directory_listing(html_content, base_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    directories = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href != '../' and not href.endswith('/'):
            record = urljoin(base_url, href)
            filename = (urlparse(record).path)
            filename_utf8 = unquote(filename)
            timestamp_str = link.next_sibling.strip().split()[0:2]
            timestamp = datetime.strptime(' '.join(timestamp_str), '%d-%b-%Y %H:%M')
            timestamp_unix = int(timestamp.timestamp())
            await save_to_temp_db([(record, filename_utf8, timestamp_unix)])
        elif href != '../':
            directories.append(href)
    return directories
    

async def fetch_and_parse_recursive(url, session):
    #print(f"Fetching and parsing: {url}")
    html_content = await fetch_directory_listing(url, session)
    directories = await parse_directory_listing(html_content, url)
    subtasks = []
    for directory in directories:
        directory_url = urljoin(url, directory)
        subtasks.append(fetch_and_parse_recursive(directory_url, session))
    await asyncio.gather(*subtasks)


async def download_file(url, filename, media_path, session):
    #print(f"Downloading: {filename}")
    async with session.get(url) as response:
        if response.status == 200:
            file_path = os.path.join(media_path, filename.lstrip('/'))
            os.umask(0)
            os.makedirs(os.path.dirname(file_path), mode=0o777, exist_ok=True)
            async with aiofiles.open(file_path, 'wb') as f:
                #print("Starting to write file...")
                await f.write(await response.content.read())
                #print("Finished writing file.")
            os.chmod(file_path, 0o777)
            print(f"Downloaded: {filename}")
        else:
            print(f"Failed to download: {filename} [Response code: {response.status}]")


async def store_in_database(db_file, files, media_path, session, download=True):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files
                 (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER)''')
    download_tasks = []
    for url, filename, timestamp in files:
        c.execute('SELECT * FROM files WHERE url = ?', (url,))
        existing_record = c.fetchone()
        if existing_record:
            if timestamp > existing_record[2] or os.path.exists(os.path.join(media_path, filename.lstrip('/'))) == False:
                if download:
                    download_tasks.append(download_file(url, filename, media_path, session))
                c.execute('UPDATE files SET filename = ?, timestamp = ? WHERE url = ?', (filename, timestamp, url))
        else:
            if download:
                download_tasks.append(download_file(url, filename, media_path, session))
            c.execute('INSERT INTO files VALUES (?, ?, ?)', (url, filename, timestamp))
    
    try:
        await asyncio.gather(*download_tasks)
    except asyncio.TimeoutError:
        print("Download tasks timed out.")
    finally:
        conn.commit()
        conn.close()


async def save_to_temp_db(files):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS files
                     (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER)''')
        c.executemany('INSERT INTO files VALUES (?, ?, ?)', files)
        conn.commit()
    finally:
        return_connection(conn)


async def create_session_and_initialize_temp_db(url):
    session = None
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        print("Fetching and parsing directory listings...")
        await fetch_and_parse_recursive(url, session)
        return session


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
        os.remove(temp_db_file)



async def main():
    parser = argparse.ArgumentParser()
    current_directory = os.path.dirname(__file__)
    default_media_path = os.path.join(current_directory, "media")
    parser.add_argument("--media", type=str, default=default_media_path, help="Path to store downloaded media files")
    parser.add_argument("--no-download", action="store_true", help="Build database without downloading files")
    args = parser.parse_args()

    temp_db_file = os.path.join(current_directory, 'temp_file_timestamps.db')
    db_file = os.path.join(current_directory, 'file_timestamps.db')

    url = 'https://emby.xiaoya.pro/'
    
    initialize_connection_pool(temp_db_file)

    await create_session_and_initialize_temp_db(url)
    await process_temp_database(temp_db_file, db_file, args.media, args)


if __name__ == "__main__":
    asyncio.run(main())
