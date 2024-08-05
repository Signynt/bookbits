import os
import csv
import glob
import sqlite3
import logging
from typing import Dict, List, Tuple
from simple_term_menu import TerminalMenu
import pyperclip
import re
from typing import List

# Constants
ANNOTATION_DB_PATTERN = "~/Library/Containers/com.apple.iBooksX/Data/Documents/AEAnnotation/AEAnnotation*.sqlite"
LIBRARY_DB_PATTERN = "~/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/BKLibrary*.sqlite"
SUPPORTED_FORMATS = ['csv', 'md']

# Set up logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

import re

def parse_cfi(cfi):
    """
    Parses an EPUB CFI into a list of components.
    """
    # Remove the leading "epubcfi(" and trailing ")"
    cfi = cfi[7:-1]
    cfi = cfi.replace(",", "")
    print(cfi)
    return re.split(r'(/|:|,)', cfi)

def cfi_to_tuple(cfi):
    """
    Converts a CFI string into a tuple that can be used for sorting.
    """
    parts = parse_cfi(cfi)
    result = []
    for part in parts:
        if part.isdigit():
            result.append(int(part))
        elif part in ('/', ':', ','):
            result.append(part)
        else:
            result.append(part)
    return tuple(result)

def sort_epubcfi(cfis):
    """
    Sorts a list of EPUB CFI strings.
    """
    return sorted(cfis, key=cfi_to_tuple)

# Example usage:
cfis = [
    "epubcfi(/6/16[chapter1]!/4,/174/2/1:0,/180/1:222)",
    "epubcfi(/6/16[chapter1]!/4/232/1,:3,:1022)",
    "epubcfi(/6/18[chapter2]!/4,/46/1:0,/48/1:497)",
    "epubcfi(/6/18[chapter2]!/4/138/1,:0,:431)",
    "epubcfi(/6/18[chapter2]!/4/142/3,:554,:970)",
    "epubcfi(/6/18[chapter2]!/4/142/3,:971,:1164)",
    "epubcfi(/6/20[chapter3]!/4/728/1,:74,:262)",
    "epubcfi(/6/20[chapter3]!/4/728/1,:263,:372)",
    "epubcfi(/6/20[chapter3]!/4,/742/3:71,/744[ch3.6]/2/1:0)"
]

sorted_cfis = sort_epubcfi(cfis)
for cfi in sorted_cfis:
    print(cfi)


def get_db_path(pattern: str) -> str:
    """
    Returns the path to a database based on the given pattern.

    Args:
        pattern (str): The glob pattern to search for the database.

    Returns:
        str: The full path to the database.

    Raises:
        FileNotFoundError: If no matching database is found.
    """
    paths = glob.glob(os.path.expanduser(pattern))
    if not paths:
        raise FileNotFoundError(f"No database found matching pattern: {pattern}")
    return paths[0]

def get_library_books() -> Dict[str, Tuple[str, str]]:
    """
    Retrieves all books from the local Apple Books library.

    Returns:
        Dict[str, Tuple[str, str]]: A dictionary where keys are asset IDs and values are tuples
                                    containing the book title and author.

    Raises:
        sqlite3.Error: If there's an issue with the database connection or query.
    """
    try:
        with sqlite3.connect(get_db_path(LIBRARY_DB_PATTERN)) as conn:
            cursor = conn.cursor()
            cursor.execute('''SELECT ZASSETID, ZSORTTITLE, ZSORTAUTHOR
                              FROM ZBKLIBRARYASSET''')
            return {row[0]: (row[1] or "Unknown Title", row[2] or "Unknown Author") for row in cursor.fetchall()}
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise

def get_library_books_with_highlights() -> List[str]:
    """
    Retrieves asset IDs of books in the local Apple Books library that have 
    highlights or annotations.

    Returns:
        List[str]: A list of asset IDs for books that have highlights or annotations.

    Raises:
        sqlite3.Error: If there's an issue with the database connection or query.
    """
    book_ids = list(get_library_books().keys())
    placeholders = ','.join('?' for _ in book_ids)
    try:
        with sqlite3.connect(get_db_path(ANNOTATION_DB_PATTERN)) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''SELECT DISTINCT ZANNOTATIONASSETID 
                              FROM ZAEANNOTATION 
                              WHERE ZANNOTATIONASSETID IN ({placeholders}) 
                              AND ZANNOTATIONSELECTEDTEXT != "";''', book_ids)
            return [entry[0] for entry in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise

def export_annotations(asset_id: str, format: str, book_title: str) -> str:
    """
    Exports annotations (highlights and notes) for the specified book to a file.

    Args:
        asset_id (str): The unique identifier for the book in the Apple Books library.
        format (str): The desired output format ('csv' or 'md').
        book_title (str): The title of the book.

    Returns:
        str: The name of the file where annotations were exported.

    Raises:
        ValueError: If an unsupported format is specified.
        sqlite3.Error: If there's an issue with the database connection or query.
        IOError: If there's an issue writing to the output file.
    """
    if format.lower() not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format! Please use one of: {', '.join(SUPPORTED_FORMATS)}")

    try:
        with sqlite3.connect(get_db_path(ANNOTATION_DB_PATTERN)) as conn:
            cursor = conn.cursor()
            #cursor.execute('''SELECT * FROM ZAEANNOTATION WHERE ZANNOTATIONASSETID = ? AND ZANNOTATIONSELECTEDTEXT != "";''', (asset_id,))
            cursor.execute('''SELECT ZANNOTATIONSELECTEDTEXT, ZANNOTATIONNOTE, ZANNOTATIONLOCATION
                              FROM ZAEANNOTATION
                              WHERE ZANNOTATIONASSETID = ? AND ZANNOTATIONSELECTEDTEXT != "";''', (asset_id,))
            #print(cursor.fetchall())
            #names = list(map(lambda x:x[0], cursor.description))
            #print(names)
            annotations = cursor.fetchall()
            print(type(annotations))
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise

    filename = f"highlights.{format.lower()}"

    try:
        annotations_dict = {"Highlight": [highlight.replace("\n", " ") for highlight, _, _ in annotations],
                            "Notes": [note.replace("\n", " ") if note else "" for _, note, _ in annotations],
                            "Locations": [location for _, _, location in annotations]}
        sorted_locations = sort_epubcfi(annotations_dict["Locations"])
        sorted_annotations = [(highlight, note, location) for location in sorted_locations for highlight, note, loc in zip(annotations_dict["Highlight"], annotations_dict["Notes"], annotations_dict["Locations"]) if loc == location]
        if format.lower() == 'csv':
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["Highlight", "Notes", "Locations"], delimiter=";")
                writer.writeheader()
                writer.writerows({"Highlight": highlight.replace("\n", " "),
                                  "Notes": note.replace("\n", " ") if note else "",
                                  "Locations": location}
                                 for highlight, note, location in sorted_annotations)
        else:  # markdown
            ##with open(filename, 'w') as mdfile:
            output_markdown = ""
            for highlight, note, location in sorted_annotations:
                output_markdown += "\n".join([f"> {line}" for line in highlight.split("\n")])
                output_markdown += f"\n\n"
                if note:
                    output_markdown += f"{note}\n\n"
                pyperclip.copy(output_markdown)
    except IOError as e:
        logging.error(f"Error writing to file: {e}")
        raise

    return filename

def main():
    try:
        book_details = get_library_books()
        books = get_library_books_with_highlights()
    except (FileNotFoundError, sqlite3.Error) as e:
        logging.error(f"Error initializing: {e}")
        print("An error occurred while accessing the Apple Books database. Please ensure Apple Books is installed and you have the necessary permissions.")
        return

    selected_book: str = None
    selected_format: str = 'md'

    def get_main_menu_items() -> List[str]:
        return [
            f"Select Book (Current: {book_details[selected_book][0] if selected_book else 'None'})",
            f"Select Format (Current: {selected_format})",
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
        SUPPORTED_FORMATS,
        title="Select Output Format",
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
            format_choice = format_menu.show()
            if format_choice is not None:
                selected_format = SUPPORTED_FORMATS[format_choice]
        elif main_choice == 2:  # Export Annotations
            if selected_book and selected_format:
                try:
                    filename = export_annotations(selected_book, selected_format, book_details[selected_book][0])
                    if selected_format == 'md':
                        print(f"Annotations copied to clipboard")
                        logging.info(f"Annotations copied to clipboard")
                    else:
                        print(f"Annotations exported to {filename}")
                        logging.info(f"Annotations exported to {filename}")
                    break
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