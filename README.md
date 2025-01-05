
# 小雅同步元数据

高速异步爬虫从 https://emby.xiaoya.pro/ 同步小雅元数据
##### [FAQ](爬虫FAQ.md)

---


## Prerequisite

Python 3.12+

Linux kernal "Max open files" need to be extended

Save it into /etc/profile.d/ to make it global avaiable after reboot

```bash
ulimit -n 4096
```
---

For Network mount, please add 'nolock' option. See [Issue#47](https://github.com/Rik-F5/xiaoya_db/issues/47#issuecomment-2162641424)

## Docker Run

**docker-cli**

```shell
docker run -d \
    --name=xiaoya-emd \
    --restart=always \
    --net=host \
    -v 媒体库目录:/media \
    -e CYCLE=86400 \
    -e RESTART_AUTO_UPDATE=false \
    ddsderek/xiaoya-emd:latest \
    --media /media
```

**docker-compose**

```yaml
version: "3"
services:
    xiaoya-emd:
        container_name: xiaoya-emd
        restart: always
        network_mode: host
        volumes:
            - 媒体库目录:/media
        environment:
            - CYCLE=86400
            - RESTART_AUTO_UPDATE=false
        image: ddsderek/xiaoya-emd:latest
        command: --media /media
```

## Installation

#### Clone the project

```bash
  git clone https://github.com/Rik-F5/xiaoya_db
```

#### Go to the project directory

```bash
  cd xiaoya_db
```

#### Switch to venv

```bash
  python3 -m venv .venv
  source .venv/bin/activate
```

#### Install modules

```bash
  python -m pip install -r requirements.txt
```
---
## Run Locally with venv activated

```bash
python solid.py --media <Xiaoya Folder>

```
---
## Usage/Examples

Download the metadata without replace the local NFOs
```bash
python solid.py --media <folder>
```

Download the metadata and replace the local NFOs if they are out of date or different

```bash
python solid.py --media <folder> --nfo true
```

Download the metadata and save the file list to a local DB (Python3.12+)

```bash
python solid.py --media <folder> --db true
```

Do not download any files. For testing or benchmark only.

```bash
python solid.py
```
---
## Parameters

```bash
  -h, --help           show this help message and exit

  --media <folder>     Path to store downloaded media files [Default: None]

  --count [number]     Max concurrent HTTP Requests [Default: 100]

  --debug, --no-debug  Verbose debug [Default: False]

  --db, --no-db        <Python3.12+ required> Save into DB [Default: False]

  --nfo, --no-nfo      Download NFO [Default: False]

  --url [url]          Download path [Default: None]

  --purge, --no-purge  Purge removed files [Default: True]

  --all, --no-all      Download all folders [Default: False]

  --location <folder>  Path to store database files [Default: None]

  --paths <file>       Bitmap of paths or a file containing paths to be selected (See paths.example)
···