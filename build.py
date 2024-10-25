import sqlite3
import os
from tqdm import tqdm
from typing import NamedTuple
import zlib
from card_parser import parse_card
from book_parser import parse_book
import subprocess
from datetime import datetime, timezone
from normalizers import normalize_reading

AOZORABUNKO_REPO_PATH = os.environ["AOZORABUNKO_REPO_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

STYLE_VERSION = "1.0.1"


class BookMetadata(NamedTuple):
    title: str
    author: str
    editor: str | None
    original_title: str | None
    subtitle: str | None
    translator: str | None


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    conn = sqlite3.connect(OUTPUT_PATH, autocommit=False)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS metadata")
    c.execute(
        "CREATE TABLE IF NOT EXISTS metadata ("
        "style TEXT NOT NULL,"
        "commit_hash TEXT NOT NULL,"
        "date TEXT NOT NULL)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS anthologies (id INTEGER PRIMARY KEY, name TEXT UNIQUE, reading TEXT)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS authors ("
        "id INTEGER PRIMARY KEY,"
        "aozora_id INTEGER NOT NULL UNIQUE,"
        "name TEXT NOT NULL,"
        "name_reading TEXT NOT NULL,"
        "name_key TEXT NOT NULL,"
        "name_roman TEXT NOT NULL,"
        "birth TEXT,"
        "death TEXT)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_authors_name ON authors (name)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_authors_name_reading ON authors (name_reading)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_authors_name_key ON authors (name_key)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_authors_name_roman ON authors (name_roman)"
    )

    c.execute(
        "CREATE TABLE IF NOT EXISTS styles (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"
    )

    c.execute(
        "CREATE TABLE IF NOT EXISTS cards ("
        "id INTEGER PRIMARY KEY,"
        "major_key INTEGER NOT NULL UNIQUE,"
        "title TEXT NOT NULL,"
        "title_reading TEXT NOT NULL,"
        "title_key TEXT NOT NULL,"
        "subtitle TEXT,"
        "subtitle_reading TEXT,"
        "subtitle_key TEXT,"
        "original_title TEXT,"
        "anthology_id INTEGER,"
        "author_id INTEGER,"
        "style_id INTEGER NOT NULL,"
        "note TEXT,"
        "first TEXT,"
        "FOREIGN KEY(anthology_id) REFERENCES anthologies(id),"
        "FOREIGN KEY(author_id) REFERENCES authors(id),"
        "FOREIGN KEY(style_id) REFERENCES styles(id))",
    )

    c.execute("CREATE INDEX IF NOT EXISTS idx_cards_title ON cards (title)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_title_reading ON cards (title_reading)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_cards_title_key ON cards (title_key)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cards_subtitle ON cards (subtitle)")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_subtitle_reading ON cards (subtitle_reading)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_subtitle_key ON cards (subtitle_key)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_original_title ON cards (original_title)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_cards_anthology_id ON cards (anthology_id)"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_cards_style_id ON cards (style_id)")

    c.execute(
        "CREATE TABLE IF NOT EXISTS card_categories ("
        "id INTEGER PRIMARY KEY,"
        "card_id INTEGER NOT NULL,"
        "category INTEGER NOT NULL,"
        "UNIQUE(card_id, category),"
        "FOREIGN KEY(card_id) REFERENCES cards(id))"
    )

    c.execute(
        "CREATE TABLE IF NOT EXISTS card_authors ("
        "id INTEGER PRIMARY KEY,"
        "card_id INTEGER NOT NULL,"
        "author_id INTEGER NOT NULL,"
        "type TEXT NOT NULL,"
        "UNIQUE(card_id, author_id, type),"
        "FOREIGN KEY(card_id) REFERENCES cards(id),"
        "FOREIGN KEY(author_id) REFERENCES authors(id))"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_card_authors_card_id ON card_authors (card_id)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_card_authors_author_id ON card_authors (author_id)"
    )

    c.execute(
        "CREATE TABLE IF NOT EXISTS books ("
        "id INTEGER PRIMARY KEY,"
        "card_id INTEGER NOT NULL,"
        "minor_key INTEGER NOT NULL,"
        "body_raw BLOB NOT NULL,"
        "body_text_rb_major TEXT NOT NULL,"
        "body_text_rt_major TEXT NOT NULL,"
        "colophon_raw BLOB NOT NULL,"
        "colophon_text TEXT NOT NULL,"
        "license TEXT,"
        "UNIQUE(card_id, minor_key),"
        "FOREIGN KEY(card_id) REFERENCES cards(id))"
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_books_card_id ON books (card_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_books_minor_key ON books (minor_key)")

    tqdm.write(f"searching for book files in {AOZORABUNKO_REPO_PATH}")

    html_files: list[str] = []
    for root, dirs, files in os.walk(os.path.join(AOZORABUNKO_REPO_PATH, "cards")):
        if os.path.basename(root) != "files":
            continue
        for file in files:
            if file.endswith(".html") and "_" in file:
                html_files += [os.path.join(root, file)]
    tqdm.write(f"found {len(html_files)} files")
    html_files.sort()

    try:
        for file_idx, filename in enumerate(tqdm(html_files)):
            try:
                author_key = int(
                    os.path.basename(os.path.dirname(os.path.dirname(filename)))
                )
                major_key, minor_key = [
                    int(x)
                    for x in os.path.splitext(os.path.basename(filename))[0].split("_")
                ]
            except Exception as e:
                continue

            c.execute("SELECT id FROM cards WHERE major_key = ?", (major_key,))
            card_id_ = c.fetchone()
            if card_id_:
                (card_id,) = card_id_
            else:
                card_info = parse_card(
                    os.path.join(
                        AOZORABUNKO_REPO_PATH,
                        "cards",
                        f"{author_key:06}",
                        f"card{major_key}.html",
                    )
                )

                if card_info.title.anthology:
                    c.execute(
                        "INSERT OR IGNORE INTO anthologies (name, reading) VALUES (?, ?)",
                        (card_info.title.anthology, card_info.title.anthology_reading),
                    )
                    c.execute(
                        "SELECT id FROM anthologies WHERE name = ?",
                        (card_info.title.anthology,),
                    )
                    (anthology_id,) = c.fetchone()
                else:
                    anthology_id = None

                c.execute(
                    "INSERT OR IGNORE INTO styles (name) VALUES (?)",
                    (card_info.info.style,),
                )
                c.execute(
                    "SELECT id FROM styles WHERE name = ?", (card_info.info.style,)
                )
                (style_id,) = c.fetchone()

                for author in card_info.authors:
                    c.execute(
                        "INSERT OR IGNORE INTO authors (aozora_id, name, name_reading, name_key, name_roman, birth, death) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            author.aozora_id,
                            author.name,
                            author.name_reading,
                            normalize_reading(author.name_reading),
                            author.name_roman,
                            author.birth,
                            author.death,
                        ),
                    )

                c.execute(
                    "SELECT id FROM authors WHERE aozora_id = ?",
                    (card_info.title.author,),
                )
                (author_id,) = c.fetchone()

                c.execute(
                    "INSERT INTO cards"
                    "(major_key, title, title_reading, title_key, subtitle, subtitle_reading, subtitle_key, original_title, anthology_id, author_id, style_id, note, first)"
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    "RETURNING id",
                    (
                        major_key,
                        card_info.title.title,
                        card_info.title.title_reading,
                        normalize_reading(card_info.title.title_reading),
                        card_info.title.subtitle,
                        card_info.title.subtitle_reading,
                        (
                            normalize_reading(card_info.title.subtitle_reading)
                            if card_info.title.subtitle_reading
                            else None
                        ),
                        card_info.title.original_title,
                        anthology_id,
                        author_id,
                        style_id,
                        card_info.info.note,
                        card_info.info.first,
                    ),
                )
                (card_id,) = c.fetchone()

                for category in card_info.info.categories:
                    c.execute(
                        "INSERT OR IGNORE INTO card_categories (card_id, category) VALUES (?, ?)",
                        (card_id, category),
                    )

                for author in card_info.authors:
                    c.execute(
                        "SELECT id FROM authors WHERE aozora_id = ?",
                        (author.aozora_id,),
                    )
                    (author_id,) = c.fetchone()

                    c.execute(
                        "INSERT INTO card_authors (card_id, author_id, type) VALUES (?, ?, ?)",
                        (card_id, author_id, author.type),
                    )

            c.execute(
                "SELECT EXISTS(SELECT * FROM books WHERE card_id = ? AND minor_key = ?)",
                (card_id, minor_key),
            )
            (exists,) = c.fetchone()
            if exists:
                continue

            try:
                book_info = parse_book(
                    os.path.join(
                        AOZORABUNKO_REPO_PATH,
                        "cards",
                        f"{author_key:06}",
                        "files",
                        f"{major_key}_{minor_key}.html",
                    )
                )
                if book_info is None:
                    continue

                # CC ライセンスに ND もしくは SA を含むならスキップ
                if book_info.license is not None:
                    cc = [x.lower() for x in book_info.license.split("/")[4].split("-")]
                    if "nd" in cc or "sa" in cc:
                        tqdm.write(f"skipped: {filename} (reason: CC {cc})")
                        continue
            except Exception as e:
                tqdm.write(f"skipped: {filename}")
                tqdm.write(f"{e}")
                continue

            c.execute(
                "INSERT INTO books "
                "(card_id, minor_key, body_raw, body_text_rb_major, body_text_rt_major, colophon_raw, colophon_text, license) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    card_id,
                    minor_key,
                    zlib.compress(book_info.body_raw.encode("utf-8")),
                    book_info.body_text_rb_major,
                    book_info.body_text_rt_major,
                    zlib.compress(book_info.colophon_raw.encode("utf-8")),
                    book_info.colophon_text,
                    book_info.license,
                ),
            )

            if file_idx > 0 and file_idx % 100 == 0:
                conn.commit()
    except Exception as e:
        print(filename)
        raise e

    conn.commit()

    # metadata
    proc = subprocess.run(
        ["git", "log", "-1", "--pretty=format:%ci\t%H"],
        capture_output=True,
        cwd=AOZORABUNKO_REPO_PATH,
        encoding="utf-8",
    )
    date, hash = proc.stdout.strip().split("\t")

    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S %z")
    date = date.astimezone(timezone.utc)
    c.execute(
        "INSERT INTO metadata (style, commit_hash, date) VALUES (?, ?, ?)",
        (STYLE_VERSION, hash, date.strftime("%Y-%m-%d %H:%M:%S %z")),
    )
    conn.commit()
    conn.close()

    print("Vacuuming...")

    conn = sqlite3.connect(OUTPUT_PATH, autocommit=True)
    conn.executescript("VACUUM")
    conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
