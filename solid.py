import argparse
import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote
import os


async def fetch_directory_listing(url, session):
    async with session.get(url) as response:
        try:
            return await response.text()
        except UnicodeDecodeError:
            print(unquote(url))
            return await response.text(errors='replace')



async def parse_directory_listing(html_content, base_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    files = []
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
            files.append((filename_utf8, record, timestamp_unix))
        elif href != '../':
            directories.append(href)
    return files, directories
    

async def fetch_and_parse_recursive(url, session):
    #print(f"Fetching and parsing: {url}")
    html_content = await fetch_directory_listing(url, session)
    files, directories = await parse_directory_listing(html_content, url)
    subtasks = []
    for directory in directories:
        directory_url = urljoin(url, directory)
        subtasks.append(fetch_and_parse_recursive(directory_url, session))
    subfiles = await asyncio.gather(*subtasks)
    for subfile_list in subfiles:
        files.extend(subfile_list)
    return files


async def download_files(files, media_path, session):
    download_tasks = []
    for file_info in files:
        url = file_info["url"]
        filename = file_info["filename"]
        download_tasks.append(asyncio.to_thread(download_file(url, filename, media_path, session)))
    await asyncio.gather(*download_tasks)

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


async def store_in_database(files, media_path, session, download=True):
    print("Storing in database...")
    conn = sqlite3.connect('file_timestamps.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files
                 (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER)''')
    
    download_tasks = []
    for filename, url, timestamp in files:
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
        await asyncio.wait_for(asyncio.gather(*download_tasks), timeout=3600)
    except asyncio.TimeoutError:
        print("Download tasks timed out.")
    finally:
        conn.commit()
        conn.close()
        print("Database storage complete.")


async def main():
    parser = argparse.ArgumentParser()
    current_directory = os.path.dirname(__file__)
    default_media_path = os.path.join(current_directory, "media")
    parser.add_argument("--media", type=str, default=default_media_path, help="Path to store downloaded media files")
    parser.add_argument("--no-download", action="store_true", help="Build database without downloading files")
    args = parser.parse_args()

    db_file = 'file_timestamps.db'
    if not os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        conn.close()
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        url = 'https://emby.xiaoya.pro/'
        print("Fetching and parsing directory listings...")
        files = await fetch_and_parse_recursive(url, session)
        print("Directory listings fetched and parsed.")
        if not args.no_download:
            await store_in_database(files, args.media, session)
        else:
            await store_in_database(files, args.media, session, False)

        print("Files and timestamps stored/updated in the database successfully.")



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
