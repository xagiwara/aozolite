import zipfile
import os
import csv
from io import TextIOWrapper
from typing import NamedTuple
import sqlite3
from normalizers import normalize_reading
from logging import getLogger

logger = getLogger(__name__)


class AuthorName(NamedTuple):
    name: str
    reading: str
    roman: str
    key: str


class AuthorInfo(NamedTuple):
    id: int
    first_name: AuthorName | None
    last_name: AuthorName
    birth_date: str | None
    death_date: str | None
    copyright_expired: bool


def load_authors(repo_path: str):
    csvzipfilepath = os.path.join(
        repo_path, "index_pages", "list_person_all_extended_utf8.zip"
    )

    authors: dict[int, AuthorInfo] = {}

    with zipfile.ZipFile(csvzipfilepath) as z:
        with z.open("list_person_all_extended_utf8.csv") as file:
            enc = "utf-8-sig" if file.read(3) == b"\xef\xbb\xbf" else "utf-8"
            file.seek(0)
            with TextIOWrapper(file, encoding=enc) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    id = int(row["人物ID"])
                    if id in authors:
                        continue
                    authors[id] = AuthorInfo(
                        id,
                        (
                            AuthorName(
                                row["名"],
                                row["名読み"],
                                row["名ローマ字"],
                                row["名読みソート用"],
                            )
                            if row["名"] != ""
                            else None
                        ),
                        (
                            AuthorName(
                                row["姓"],
                                row["姓読み"],
                                row["姓ローマ字"],
                                row["姓読みソート用"],
                            )
                            if row["姓"] != ""
                            else None
                        ),
                        row["生年月日"] if row["生年月日"] != "" else None,
                        row["没年月日"] if row["没年月日"] != "" else None,
                        {"あり": False, "なし": True}[row["人物著作権フラグ"]],
                    )
    return list(authors.values())


def create_tables(conn: sqlite3.Connection):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS authors ("
        "   id INTEGER UNIQUE,"
        "   name TEXT NOT NULL,"
        "   name_reading TEXT NOT NULL,"
        "   name_roman TEXT NOT NULL,"
        "   name_key TEXT NOT NULL,"
        "   birth TEXT,"
        "   death TEXT,"
        "   copyright_expired BOOLEAN NOT NULL"
        ")"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS authors_name ON authors (name)")
    conn.execute("CREATE INDEX IF NOT EXISTS authors_name_key ON authors (name_key)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS authors_name_reading ON authors (name_reading)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS authors_name_roman ON authors (name_roman)"
    )


def write_rows(conn: sqlite3.Connection, authors: list[AuthorInfo]):
    conn.executemany(
        "INSERT OR REPLACE INTO authors (id, name, name_reading, name_roman, name_key, birth, death, copyright_expired) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (
                author.id,
                (
                    author.last_name.name + "\u3000" + author.first_name.name
                    if author.first_name
                    else author.last_name.name
                ),
                (
                    author.last_name.reading + "\u3000" + author.first_name.reading
                    if author.first_name
                    else author.last_name.reading
                ),
                (
                    author.first_name.roman + " " + author.last_name.roman
                    if author.first_name
                    else author.last_name.roman
                ),
                normalize_reading(
                    author.last_name.key + author.first_name.key
                    if author.first_name
                    else author.last_name.key
                ),
                author.birth_date,
                author.death_date,
                author.copyright_expired,
            )
            for author in authors
        ),
    )
    conn.commit()


if __name__ == "__main__":
    logger.setLevel("INFO")

    logger.info("Loading authors...")
    authors = load_authors(os.environ["AOZORABUNKO_REPO_PATH"])
    print(set([x.style for x in authors]))

    logger.info("Opening database...")
    with sqlite3.connect(os.environ["OUTPUT_PATH"], autocommit=False) as conn:
        logger.info("Creating tables...")
        create_tables(conn)
        logger.info("sorting rows...")
        authors = list(authors)
        authors.sort(key=lambda x: x.id)
        logger.info("Writing rows...")
        write_rows(conn, authors)
    logger.info("Done.")
