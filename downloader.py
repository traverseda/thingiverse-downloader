"""A downloader for thingiverse. Downloads metadata

Highly recomended to use it with a filesystem that supports
in-line compression, like btrfs. A lot of this data compresses very well,
the metada index compresses ~90% under btrfs+zstd. We generate a bunch of
HTML files for viewing the objects, those will also compress very well.
"""

import json
from tqdm import tqdm
import hug
import requests
import sys, os
from pathlib import Path

#Reduce process priority
try: os.nice(10)
except: pass

from zict import LMDB,Func
db = LMDB('metadata.lmdb')
#import zlib
#db = Func(zlib.compress, zlib.decompress, db)
db = Func(lambda x: x.encode('utf-8'), lambda x: x.decode("utf-8"), db)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Authorization': 'Bearer 56edfc79ecf25922b98202dd79a291aa',
    'Origin': 'https://www.',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'https://www.thingiverse.com/',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-ca',
    'TE': 'Trailers',
}

def getLatestObjectId():
    """Finds the last object uploaded to thingiverse
    """
    latestUrl = "https://api.thingiverse.com/search/?page=1&per_page=20&sort=newest&type=things"
    latest = requests.get(latestUrl, headers=headers)
    latest = json.loads(latest.content)['hits'][0]['id']
    return latest

def getLastDownloadId():
    "return the ID of the last file you downloaded"
    import lmdb
    env = lmdb.open("./metadata.lmdb")
    with env.begin(write=True) as txn:
        cursor = txn.cursor()
        if cursor.last(): return int(cursor.key())
    return 0

def downloadMetadata(u):
    import requests
    try:
        item = requests.get("https://api.thingiverse.com/things/"+str(u),headers=headers)
        itemData = json.loads(item.content)
        if isinstance(itemData,list):
            itemData=itemData[0]
        if "id" not in itemData:
            itemData['id']=u
        return  u, json.dumps(itemData)
    except Exception as e:
        return u, json.dumps({"id":u,"error":str("e")})

@hug.cli()
def put(thingId:int,content:str=None):
    """Replace the metadata for this object

    Please note that replacing data out-of-order can be
    slow, there's probably no reason to ever use this command.
    """
    if not content:
        print("Waiting for stdin to close...")
        content = sys.stdin.read()
    json.loads(content) #Make sure content is json-loadable
    thingId = str(thingId).zfill(8)
    db[thingId]=content

@hug.cli(output=hug.output_format.pretty_json)
def get(offset:int,limit:int=1):
    """Get metadata for these objects"""
    for thingId in range(offset,offset+limit):
        thingId = str(thingId).zfill(8)
        yield json.loads(db[thingId])

def downloadFiles(metadata):
    thingId,data = metadata
    if not "files_url" in data:
        return
    thingId = thingId.zfill(9)
    files = Path("things")/thingId[0:3]/thingId[3:6]/thingId[6:9]/"files"
    files.mkdir(parents=True, exist_ok=True)
    thumbs = Path("things")/thingId[0:3]/thingId[3:6]/thingId[6:9]/"thumbnails"
    thumbs.mkdir(parents=True, exist_ok=True)
    data = json.loads(data)
    url=data["files_url"]
    items = requests.get(url,headers=headers)
    try:
        for item in items.json():
            with (files/item['name']).open("wb") as f:
                f.write(requests.get(item['public_url']).content)
            assert item['thumbnail'].endswith(".jpg")
            if "default_image" in item and item["default_image"]:
                for i in item['default_image']['sizes']:
                    if i['type']=='preview' and i['size'] == 'featured':
                        thumburl = i['url']
                        break
                with (thumbs/(item['name']+".jpg")).open("wb") as f:
                    f.write(requests.get(thumburl).content)
    except Exception as e:
        print(thingId,str(e),item)
    return

@hug.cli()
def download_files(poolsize:int=20):
    from multiprocessing import set_start_method
    set_start_method("spawn")
    from multiprocessing import Pool
    pool = Pool(processes=poolsize)
    results = pool.imap(downloadFiles, db.items())
#    results = (downloadFiles(*item) for item in db.items())
    for item in tqdm(results,total=len(db)):
        pass

@hug.cli()
def download_metadata(poolsize:int=20):
    """Download/update metaddata file.
    """
    latest = getLatestObjectId()
    lastDownloaded = getLastDownloadId()
    from multiprocessing import set_start_method
    set_start_method("spawn")

    total = latest-lastDownloaded+1
    print(f"Downloading metadata for things {getLastDownloadId()+1} to {latest} ({total} items)")
    from multiprocessing import Pool
    pool = Pool(processes=poolsize)
    results = pool.imap(downloadMetadata, range(lastDownloaded+1,latest+1))
    for thingId, result in tqdm(results,total=total):
        thingId = str(thingId).zfill(8)
        db[thingId]=result

def main():
    hug.API(__name__).cli()

if __name__ == '__main__':
    main()
