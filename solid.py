import argparse
import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote
import os

# Function to fetch HTML content of the directory listing asynchronously
async def fetch_directory_listing(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            try:
                return await response.text()
            except UnicodeDecodeError:
                print(unquote(url))
                return await response.text(errors='replace')

# Asynchronous function to parse HTML and extract file names and timestamps
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

# Asynchronous function to fetch and parse directory listings recursively
async def fetch_and_parse_recursive(url):
    html_content = await fetch_directory_listing(url)
    files, directories = await parse_directory_listing(html_content, url)
    print(f"Parsed: {unquote(url)}")
    subtasks = []
    for directory in directories:
        directory_url = urljoin(url, directory)
        subtasks.append(fetch_and_parse_recursive(directory_url))
    subfiles = await asyncio.gather(*subtasks)
    for subfile_list in subfiles:
        files.extend(subfile_list)
    return files

# Asynchronous function to download a file
async def download_file(url, filename, media_path):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                # Define the file path to save
                file_path = os.path.join(media_path, filename.lstrip('/'))
                # Ensure the directory for the file exists
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                # Save the file
                async with aiofiles.open(file_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        await f.write(chunk)
                print(f"Downloaded: {filename}")
            else:
                print(f"Failed to download: {filename}")



# Function to store file names, timestamps, and download files if newer or not exist
async def store_in_database(files, media_path, download=True):
    conn = sqlite3.connect('file_timestamps.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files
                 (url TEXT PRIMARY KEY, filename TEXT, timestamp INTEGER)''')
    for filename, url, timestamp in files:
        c.execute('SELECT * FROM files WHERE url = ?', (url,))
        existing_record = c.fetchone()
        if existing_record:
            if timestamp > existing_record[2] or not os.path.exists(os.path.join(media_path, filename.lstrip('/'))):
                if download == True:
                    await download_file(url, filename, media_path)
                c.execute('UPDATE files SET filename = ?, timestamp = ? WHERE url = ?', (filename, timestamp, url))
        else:
            if download == True:
                await download_file(url, filename, media_path)
            c.execute('INSERT INTO files VALUES (?, ?, ?)', (url, filename, timestamp))
    conn.commit()
    conn.close()

# Main function
async def main():
    parser = argparse.ArgumentParser()
    # Get the directory path of the current Python file
    current_directory = os.path.dirname(__file__)
    # Append "media" to the current directory path
    default_media_path = os.path.join(current_directory, "media")
    parser.add_argument("--media", type=str, default=default_media_path, help="Path to store downloaded media files")
    parser.add_argument("--no-download", action="store_true", help="Build database without downloading files")
    args = parser.parse_args()

    db_file = 'file_timestamps.db'
    if not os.path.exists(db_file):
        # If the database file doesn't exist, create a new one
        conn = sqlite3.connect(db_file)
        conn.close()

    url = 'https://emby.xiaoya.pro/%E6%AF%8F%E6%97%A5%E6%9B%B4%E6%96%B0/%E5%8A%A8%E6%BC%AB/%E6%97%A5%E6%9C%AC/2011/'
    files = await fetch_and_parse_recursive(url)
    if not args.no_download:
        await store_in_database(files, args.media)
    else:
        await store_in_database(files, args.media, False)

    print("Files and timestamps stored/updated in the database successfully.")

if __name__ == "__main__":
    asyncio.run(main())
