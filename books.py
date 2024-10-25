import zipfile
import os
import csv
from io import TextIOWrapper
from typing import NamedTuple
import sqlite3
from normalizers import normalize_reading, normalize_text
from logging import getLogger
import re

logger = getLogger(__name__)

url_author_pattern = re.compile(r".*/cards/([0-9]+)/card[0-9]+\.html")


class BookAuthor(NamedTuple):
    id: int
    role: str


class BookInfo(NamedTuple):
    id: int
    title: str
    title_reading: str
    title_key: str
    subtitle: str | None
    subtitle_reading: str | None
    original_title: str | None
    first: str | None
    copyright_expired: bool
    style: str
    author_id: int
    authors: list[BookAuthor]


def load_books(repo_path: str):
    csvzipfilepath = os.path.join(
        repo_path, "index_pages", "list_person_all_extended_utf8.zip"
    )

    books: dict[int, BookInfo] = {}

    with zipfile.ZipFile(csvzipfilepath) as z:
        with z.open("list_person_all_extended_utf8.csv") as file:
            enc = "utf-8-sig" if file.read(3) == b"\xef\xbb\xbf" else "utf-8"
            file.seek(0)
            with TextIOWrapper(file, encoding=enc) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    id = int(row["作品ID"])
                    if id not in books:
                        author_id_match = re.match(
                            url_author_pattern, row["図書カードURL"]
                        )
                        books[id] = BookInfo(
                            id,
                            row["作品名"],
                            row["作品名読み"],
                            row["ソート用読み"],
                            row["副題"] if row["副題"] != "" else None,
                            row["副題読み"] if row["副題読み"] != "" else None,
                            row["原題"] if row["原題"] != "" else None,
                            row["初出"] if row["初出"] != "" else None,
                            {"あり": False, "なし": True}[row["作品著作権フラグ"]],
                            row["文字遣い種別"],
                            int(author_id_match.group(1)),
                            [],
                        )
                    books[id].authors.append(
                        BookAuthor(int(row["人物ID"]), row["役割フラグ"])
                    )

    return list(books.values())


def create_tables(conn: sqlite3.Connection):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS styles (id PRIMARY KEY, style TEXT NOT NULL UNIQUE)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER UNIQUE,
            title TEXT NOT NULL,
            title_reading TEXT NOT NULL,
            title_key TEXT NOT NULL,
            subtitle TEXT,
            subtitle_reading TEXT,
            original_title TEXT,
            first TEXT,
            author_id INTEGER NOT NULL,
            copyright_expired BOOLEAN NOT NULL,
            style_id INTEGER NOT NULL,
            FOREIGN KEY (style_id) REFERENCES styles (id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS books_title ON books (title)")
    conn.execute("CREATE INDEX IF NOT EXISTS books_title_key ON books (title_key)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS books_title_reading ON books (title_reading)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS books_subtitle ON books (subtitle)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS books_subtitle_reading ON books (subtitle_reading)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS books_original_title ON books (original_title)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS books_first ON books (first)")
    conn.execute("CREATE INDEX IF NOT EXISTS books_author_id ON authors (id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS author_roles (
            id INTEGER PRIMARY KEY,
            role TEXT UNIQUE NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS card_authors (
            id INTEGER PRIMARY KEY,
            card_id INTEGER NOT NULL,
            author_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            FOREIGN KEY (card_id) REFERENCES books (id),
            FOREIGN KEY (author_id) REFERENCES authors (id),
            FOREIGN KEY (role_id) REFERENCES author_roles (id))
        """
    )
    # conn.execute("CREATE INDEX IF NOT EXISTS books_copyright_expired ON books (copyright_expired)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS card_authors_card_id ON card_authors (card_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS card_authors_author_id ON card_authors (author_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS card_authors_role_id ON card_authors (role_id)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS author_roles_role ON author_roles (role)")


def write_rows(conn: sqlite3.Connection, books: list[BookInfo]):
    conn.executemany(
        "INSERT OR REPLACE INTO author_roles (id, role) VALUES (?, ?)",
        [(1, "著者"), (2, "編者"), (3, "翻訳者"), (4, "校訂者"), (5, "その他")],
    )
    conn.executemany(
        "INSERT OR REPLACE INTO styles (id, style) VALUES (?, ?)",
        (
            (1, "新字新仮名"),
            (2, "新字旧仮名"),
            (3, "旧字新仮名"),
            (4, "旧字旧仮名"),
            (5, "その他"),
        ),
    )
    conn.executemany(
        "INSERT OR REPLACE INTO books (id, title, title_reading, title_key, subtitle, subtitle_reading, original_title, first, copyright_expired, author_id, style_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT id FROM styles WHERE style = ?))",
        (
            (
                card.id,
                normalize_text(card.title),
                card.title_reading,
                normalize_reading(card.title_key),
                normalize_text(card.subtitle) if card.subtitle else None,
                card.subtitle_reading,
                card.original_title,
                card.first,
                card.copyright_expired,
                card.author_id,
                card.style,
            )
            for card in books
        ),
    )
    conn.executemany(
        "INSERT OR REPLACE INTO card_authors (card_id, author_id, role_id) VALUES (?, ?, (SELECT id FROM author_roles WHERE role = ?))",
        (
            (
                card.id,
                author.id,
                author.role,
            )
            for card in books
            for author in card.authors
        ),
    )
    conn.commit()


if __name__ == "__main__":
    logger.setLevel("INFO")

    logger.info("Loading books...")
    books = load_books(os.environ["AOZORABUNKO_REPO_PATH"])
    books.sort(key=lambda x: x.id)

    logger.info("Writing books to database...")
    with sqlite3.connect(os.environ["OUTPUT_PATH"], autocommit=False) as conn:
        logger.info("Creating tables...")
        create_tables(conn)
        logger.info("Writing rows...")
        write_rows(conn, books)
    logger.info("Done")
