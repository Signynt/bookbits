import pathlib
import sqlite3
import functools
import subprocess
from time import sleep
from tqdm import tqdm

from typing import (List, Dict, Union)


SqliteQueryType = List[Dict[str, Union[str, int]]]

ANNOTATION_DB_PATH = (
    pathlib.Path.home() /
    "Library/Containers/com.apple.iBooksX/Data/Documents/AEAnnotation/"
)
BOOK_DB_PATH = (
    pathlib.Path.home() /
    "Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/"
)

BOOKS_APP_PATH = (
    pathlib.Path.home() /
    "/System/Applications/Books.app"
)

BOOKS_APP_NAME = "Books"


ATTACH_BOOKS_QUERY = """
attach database ? as books
"""


NOTE_LIST_FIELDS = [
    'asset_id',
    'title',
    'author',
    'location',
    'selected_text',
    'note',
    'represent_text',
    'chapter',
    'style',
    'modified_date'
]

NOTE_LIST_QUERY = """
select 
ZANNOTATIONASSETID as asset_id, 
books.ZBKLIBRARYASSET.ZTITLE as title, 
books.ZBKLIBRARYASSET.ZAUTHOR as author,
ZANNOTATIONLOCATION as location,
ZANNOTATIONSELECTEDTEXT as selected_text, 
ZANNOTATIONNOTE as note,
ZANNOTATIONREPRESENTATIVETEXT as represent_text, 
ZFUTUREPROOFING5 as chapter, 
ZANNOTATIONSTYLE as style,
ZANNOTATIONMODIFICATIONDATE as modified_date

from ZAEANNOTATION

left join books.ZBKLIBRARYASSET
on ZAEANNOTATION.ZANNOTATIONASSETID = books.ZBKLIBRARYASSET.ZASSETID

where ZANNOTATIONDELETED = 0 and (title not null and author not null) and ((selected_text != '' and selected_text not null) or note not null)

order by ZANNOTATIONASSETID, ZPLLOCATIONRANGESTART;
"""


@functools.lru_cache(maxsize=1)
def get_ibooks_database() -> sqlite3.Cursor:
    
    sqlite_files = list(ANNOTATION_DB_PATH.glob("*.sqlite"))

    if len(sqlite_files) == 0:
        raise FileNotFoundError("iBooks database not found")
    else:
        sqlite_file = sqlite_files[0]

    assets_files = list(BOOK_DB_PATH.glob("*.sqlite"))

    if len(assets_files) == 0:
        raise FileNotFoundError("iBooks assets database not found")
    else:
        assets_file = assets_files[0]

    db1 = sqlite3.connect(str(sqlite_file), check_same_thread=False)
    cursor = db1.cursor()
    cursor.execute(
        ATTACH_BOOKS_QUERY,
        (str(assets_file),)
    )

    return cursor


def fetch_annotations(refresh: bool, sleep_time: int = 20) -> SqliteQueryType:
    # refresh database by opening Books and waiting
    if refresh:
        subprocess.run(f"open {BOOKS_APP_PATH}".split())
        print("Refreshing database...")
        for i in tqdm(range(sleep_time)):
            sleep(1)
        subprocess.run(["osascript", "-e" , f'quit app "{BOOKS_APP_NAME}"'])
    cur = get_ibooks_database()
    exe = cur.execute(NOTE_LIST_QUERY)
    res = exe.fetchall()
    annos = [dict(zip(NOTE_LIST_FIELDS, r)) for r in res]

    return annos
