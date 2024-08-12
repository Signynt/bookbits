import pathlib
import datetime as dt
from typing import List, Dict, Optional, Any, Callable, Tuple
from apple_books_highlights.booksdb import SqliteQueryType
import subprocess
from time import sleep
from tqdm import tqdm
import sqlite3
import pyperclip
from simple_term_menu import TerminalMenu
import logging
import re
import sys

TITLE_OPTIONS = ['yes', 'no']
NS_TIME_INTERVAL_SINCE_1970 = 978307200

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

def parse_epubcfi(raw: str) -> List[int]:

    if raw is None:
        return []

    parts = raw[8:-1].split(',')
    cfistart = parts[0] + parts[1]

    parts = cfistart.split(':')

    path = parts[0]
    offsets = [
        int(x[1:])
        for x in re.findall('(/\d+)', path)
    ]

    if len(parts) > 1:
        offsets.append(int(parts[1]))

    return offsets

def epubcfi_compare(x: List[int], y: List[int]) -> int:
    depth = min(len(x), len(y))
    for d in range(depth):
        if x[d] == y[d]:
            continue
        else:
            return x[d] - y[d]

    return len(x) - len(y)

def query_compare_no_asset_id(x: Dict[str, str], y: Dict[str, str]) -> int:
    return epubcfi_compare(
        parse_epubcfi(x['location']),
        parse_epubcfi(y['location'])
    )

def cmp_to_key(mycmp: Callable) -> Any:
    'Convert a cmp= function into a key= function'
    class K:
        def __init__(self, obj: Any, *args: Any) -> None:
            self.obj = obj

        def __lt__(self, other: Any) -> Any:
            return mycmp(self.obj, other.obj) < 0

        def __gt__(self, other: Any) -> Any:
            return mycmp(self.obj, other.obj) > 0

        def __eq__(self, other: Any) -> Any:
            return mycmp(self.obj, other.obj) == 0

        def __le__(self, other: Any) -> Any:
            return mycmp(self.obj, other.obj) <= 0

        def __ge__(self, other: Any) -> Any:
            return mycmp(self.obj, other.obj) >= 0

        def __ne__(self, other: Any) -> Any:
            return mycmp(self.obj, other.obj) != 0
    return K

class Annotation(object):

    def __init__(self, location: str, selected_text: str=None, 
                 note: str=None, represent_text: str=None, chapter: str=None, 
                 style: str=None, modified_date: dt.datetime=None) -> None:

        if (selected_text is None) and (note is None):
            raise ValueError('specify either selected_text or note')
        
        stripspaces = lambda x : x.strip() if x else x
        self.location = location
        self.selected_text = stripspaces(selected_text)

        self.represent_text = stripspaces(represent_text)

        self.chapter = chapter
        self.style = style
        self.note = stripspaces(note)
        self.modified_date = modified_date

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

def create_annotation(location: str, selected_text: str=None, 
                      note: str=None, represent_text: str=None, chapter: str=None, 
                      style: str=None, modified_date: dt.datetime=None) -> Annotation:
    if (selected_text is None) and (note is None):
        raise ValueError('specify either selected_text or note')
    
    stripspaces = lambda x : x.strip() if x else x
    annotation = Annotation(
        location=location,
        selected_text=stripspaces(selected_text),
        represent_text=stripspaces(represent_text),
        chapter=chapter,
        style=style,
        note=stripspaces(note),
        modified_date=modified_date
    )
    return annotation

def populate_annotations(annos: SqliteQueryType):
    res = [
        r
        for r in annos
        if r['asset_id'] is not None and
        ((r['selected_text'] is not None) or (r['note'] is not None))
    ]

    anno_group: Dict[str, List[Annotation]] = {}
    for r in res:
        asset_id = str(r['asset_id'])
        if asset_id not in anno_group:
            anno_group[asset_id] = []

        location = str(r['location']) if r['location'] else None
        selected_text = (
            str(r['selected_text']) if r['selected_text'] else None)
        note = str(r['note']) if r['note'] else None
        represent_text = (
            str(r['represent_text']) if r['represent_text'] else None)
        chapter = str(r['chapter']) if r['chapter'] else None
        style = str(r['style']) if r['style'] else None

        anno = create_annotation(
            location=location,
            selected_text=selected_text,
            note=note,
            represent_text=represent_text,
            chapter=chapter,
            style=style,
            modified_date=dt.datetime.fromtimestamp(
                NS_TIME_INTERVAL_SINCE_1970 + int(r['modified_date'])),
        )
        anno_group[asset_id].append(anno)

    return(anno_group)

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
        subprocess.run(["osascript", "-e" , f'run app "{BOOKS_APP_NAME}"'])
        print("Refreshing database...")
        for i in tqdm(range(sleep_time)):
            sleep(1)
        subprocess.run(["osascript", "-e" , f'quit app "{BOOKS_APP_NAME}"'])
    cur = get_ibooks_database()
    exe = cur.execute(NOTE_LIST_QUERY)
    res = exe.fetchall()
    annos = [dict(zip(NOTE_LIST_FIELDS, r)) for r in res]

    return annos

def extract_chapter_title(location: str) -> Optional[str]:
    chapter_title_match = re.search(r'\[(.*?)\]', location)
    if chapter_title_match:
        extracted_text = chapter_title_match.group(1)
        extracted_text = re.sub(r'(\w)([0-9])', r'\1 \2', extracted_text)
        extracted_text = re.sub(r'(\D)([0-9])', r'\1 \2', extracted_text)
        extracted_text = re.sub(r'(\d)(\D)', r'\1 \2', extracted_text)
        extracted_text = extracted_text.capitalize()
        extracted_title = re.sub(r'\.[^.]*$', '', extracted_text)

        return extracted_title

def content(annotations, export_titles) -> str:
    md = ""
    annotations.sort(key=cmp_to_key(query_compare_no_asset_id))
    current_chapter = ''
    for anno in annotations:
        if anno.selected_text is not None:
            if export_titles == 'yes':
                extracted_title = extract_chapter_title(anno.location)
                if extracted_title != current_chapter:
                    if extracted_title != '':
                        md += f"# {extracted_title}\n"
                current_chapter = extracted_title

            text_without_excess_spaces = re.sub(r'\s{2,}', ' ', anno.selected_text)
            text_with_blockquote = '> ' + text_without_excess_spaces.replace('\n', '\n> ')
            md += f"{text_with_blockquote}\n\n"
        if anno.note is not None:
            md += f"{anno.note}\n\n"
    return md

def get_library_books() -> Dict[str, Tuple[str, str]]:
    try:
        cursor = get_ibooks_database()
        #with sqlite3.connect(get_db_path(LIBRARY_DB_PATTERN)) as conn:
            #cursor = conn.cursor()
        cursor.execute('''SELECT ZASSETID, ZSORTTITLE, ZSORTAUTHOR
                              FROM ZBKLIBRARYASSET''')
        return {row[0]: (row[1] or "Unknown Title", row[2] or "Unknown Author") for row in cursor.fetchall()}
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise

def main():
    try:
        refresh = True if "--refresh" in sys.argv else False
        annos = fetch_annotations(refresh=refresh)
        anno_group = populate_annotations(annos)
        book_details = get_library_books()
        books = list(anno_group.keys())
    except (FileNotFoundError, sqlite3.Error) as e:
        logging.error(f"Error initializing: {e}")
        print("An error occurred while accessing the Apple Books database. Please ensure Apple Books is installed and you have the necessary permissions.")
        return

    selected_book: str = None
    title_option: str = 'yes'

    def get_main_menu_items() -> List[str]:
        return [
            f"Select Book (Current: {book_details[selected_book][0] if selected_book else 'None'})",
            f"Select Title Export (Current: {title_option})",
            "Export Annotations",
            "Quit"
        ]

    main_menu_title = "BookBits - Apple Books Highlight Exporter"

    book_menu = TerminalMenu(
        [f"{book_details[book][0]} by {book_details[book][1]}" for book in books],
        title="Select a Book",
        menu_cursor=">> ",
        menu_cursor_style=("fg_red", "bold"),
        menu_highlight_style=("bg_red", "fg_black"),
    )

    format_menu = TerminalMenu(
        TITLE_OPTIONS,
        title="Select If Chapter Titles Should Be Exported",
        menu_cursor=">> ",
        menu_cursor_style=("fg_red", "bold"),
        menu_highlight_style=("bg_red", "fg_black"),
    )

    while True:
        main_menu = TerminalMenu(
            get_main_menu_items(),
            title=main_menu_title,
            menu_cursor=">> ",
            menu_cursor_style=("fg_red", "bold"),
            menu_highlight_style=("bg_red", "fg_black"),
        )
        
        main_choice = main_menu.show()

        if main_choice == 0:  # Select Book
            book_choice = book_menu.show()
            if book_choice is not None:
                selected_book = books[book_choice]
        elif main_choice == 1:  # Select Format
            export_titles = format_menu.show()
            if export_titles is not None:
                title_option = TITLE_OPTIONS[export_titles]
        elif main_choice == 2:  # Export Annotations
            if selected_book and title_option:
                try:
                    output_highlights = content(anno_group[selected_book], title_option)
                    pyperclip.copy(output_highlights)
                    if not refresh: break
                except (ValueError, sqlite3.Error, IOError) as e:
                    print(f"Error exporting annotations: {e}")
                    logging.error(f"Error exporting annotations: {e}")
            else:
                print("Please select a book before exporting!")
        elif main_choice == 3 or main_choice is None:  # Quit
            print("Exiting...")
            break

if __name__ == "__main__":
    main()