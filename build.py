import sqlite3
import os
from tqdm import tqdm
from typing import NamedTuple
import zlib
import subprocess
from datetime import datetime, timezone
from logging import getLogger
from tqdm.contrib.logging import logging_redirect_tqdm
from tempfile import gettempdir
from authors import (
    load_authors,
    create_tables as create_authors_table,
    write_rows as write_authors,
)
from books import (
    load_books,
    create_tables as create_books_table,
    write_rows as write_books,
)
from book_texts import (
    parse_book,
    find_files as find_book_files,
    create_tables as create_book_texts_table,
)
import shutil


STYLE_VERSION = "2.0.0"

logger = getLogger(__name__)


class BookMetadata(NamedTuple):
    title: str
    author: str
    editor: str | None
    original_title: str | None
    subtitle: str | None
    translator: str | None


def main(aozorabunko_repo_path: str, output_dir: str, temp_dir: str, **kwargs):
    kwargs

    # read metadata
    proc = subprocess.run(
        ["git", "log", "-1", "--pretty=format:%ci\t%H"],
        capture_output=True,
        cwd=aozorabunko_repo_path,
        encoding="utf-8",
    )
    date, hash = proc.stdout.strip().split("\t")

    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S %z")
    date = date.astimezone(timezone.utc)
    tmp_filepath = os.path.join(
        temp_dir,
        STYLE_VERSION,
        f"{date.strftime("%Y%m%d-%H%M%S")}_{hash[:6]}.sqlite3",
    )
    output_filepath = os.path.join(
        output_dir,
        STYLE_VERSION,
        f"{date.strftime("%Y%m%d-%H%M%S")}_{hash[:6]}.sqlite3",
    )

    if os.path.exists(output_filepath):
        logger.info(f"already exists: {tmp_filepath}")
        return

    os.makedirs(os.path.dirname(tmp_filepath), exist_ok=True)
    conn = sqlite3.connect(tmp_filepath, autocommit=False)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metadata ("
        "style TEXT NOT NULL,"
        "commit_hash TEXT NOT NULL,"
        "date TEXT NOT NULL)"
    )
    create_authors_table(conn)
    create_books_table(conn)
    create_book_texts_table(conn)

    c = conn.cursor()

    # metadata
    c.execute("DELETE FROM metadata")
    c.execute(
        "INSERT INTO metadata (style, commit_hash, date) VALUES (?, ?, ?)",
        (STYLE_VERSION, hash, date.strftime("%Y-%m-%d %H:%M:%S %z")),
    )
    conn.commit()

    # authors
    logger.info("loading authors...")
    authors = load_authors(aozorabunko_repo_path)
    authors.sort(key=lambda x: x.id)
    logger.info(f"loaded {len(authors)} authors")
    logger.info("writing authors...")
    write_authors(conn, authors)
    logger.info("done: writing authors")

    # books
    logger.info("loading books...")
    books = load_books(aozorabunko_repo_path)
    books.sort(key=lambda x: x.id)
    logger.info(f"loaded {len(books)} books")
    logger.info("writing books...")
    write_books(conn, books)
    logger.info("done: writing books")

    # book texts
    logger.info("finding book files...")
    book_files = find_book_files(aozorabunko_repo_path)
    logger.info(f"found {len(book_files)} files")
    logger.info("writing book texts...")
    try:
        for i, (author_id, book_id, version_id) in enumerate(tqdm(book_files)):
            c.execute(
                "SELECT EXISTS(SELECT 1 FROM book_texts WHERE book_id = ? AND revision = ?)",
                (book_id, version_id),
            )
            (exists,) = c.fetchone()
            if exists:
                continue

            if i % 100 == 0:
                conn.commit()

            book_filepath = os.path.join(
                aozorabunko_repo_path,
                "cards",
                f"{author_id:06}",
                "files",
                f"{book_id}_{version_id}.html",
            )
            try:
                book = parse_book(book_filepath)

                if book is None:
                    logger.info(f"skipped: {book_filepath} (reason: body not found)")
                    continue

                # CC ライセンスに ND もしくは SA を含むならスキップ
                if book.license is not None:
                    cc = [x.lower() for x in book.license.split("/")[4].split("-")]
                    if "nd" in cc or "sa" in cc:
                        logger.info(f"skipped: {book_filepath} (reason: CC {cc})")
                        continue

                c.execute(
                    "INSERT INTO book_texts (book_id, revision, body_raw, body_text_rb_major, body_text_rt_major, colophon_raw, colophon_text, license) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        book_id,
                        version_id,
                        zlib.compress(book.body_raw.encode("utf-8")),
                        book.body_text_rb_major,
                        book.body_text_rt_major,
                        zlib.compress(book.colophon_raw.encode("utf-8")),
                        book.colophon_text,
                        book.license,
                    ),
                )
            except Exception as e:
                logger.error(f"skipped: {book_filepath}")
                logger.error(e)
                continue

    except Exception as e:
        logger.error(
            os.path.join(
                aozorabunko_repo_path,
                "cards",
                f"{author_id:06}",
                "files",
                f"{book_id}_{version_id}.html",
            )
        )
        raise e

    conn.commit()

    logger.info("Claning up...")
    c.execute(
        """
        SELECT book_texts.book_id, book_texts.revision FROM book_texts
	        INNER JOIN books ON books.id = book_texts.book_id
	        WHERE copyright_expired = 0 AND license IS NULL"""
    )
    remove_texts = c.fetchall()
    c.executemany(
        "DELETE FROM book_texts WHERE book_id = ? AND revision = ?", remove_texts
    )
    conn.commit()
    logger.info(f"Removed {len(remove_texts)} texts")

    c.execute(
        """
        SELECT books.id
	        FROM book_texts
	        RIGHT JOIN books ON books.id = book_texts.book_id
	        WHERE book_id IS NULL"""
    )
    remove_books = [x for x, in c.fetchall()]
    c.executemany(
        "DELETE FROM book_authors WHERE book_id = ?", [(x,) for x in remove_books]
    )
    c.executemany("DELETE FROM books WHERE id = ?", [(x,) for x in remove_books])
    conn.commit()
    logger.info(f"Removed {len(remove_books)} books")

    c.execute(
        """
        SELECT authors.id
            FROM book_authors
            RIGHT JOIN authors ON authors.id = book_authors.author_id
            WHERE book_authors.id IS NULL"""
    )
    remove_authors = [x for x, in c.fetchall()]
    c.executemany("DELETE FROM authors WHERE id = ?", [(x,) for x in remove_authors])
    conn.commit()
    logger.info(f"Removed {len(remove_authors)} authors")
    conn.close()

    logger.info("Vacuuming...")
    conn = sqlite3.connect(tmp_filepath, autocommit=True)
    conn.executescript("VACUUM")
    conn.close()

    logger.info("Moving...")
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    shutil.move(tmp_filepath, output_filepath)
    shutil.rmtree(os.path.dirname(tmp_filepath))
    logger.info("Done.")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--temp_dir", default=None)
    parser.add_argument("--aozorabunko-repo-path", default=None)
    args = parser.parse_args()

    if args.output_dir is None:
        args.output_dir = os.environ["OUTPUT_DIR"]
    if args.temp_dir is None:
        args.temp_dir = os.environ.get(
            "TEMP_DIR", os.path.join(gettempdir(), "aozolite")
        )
    if args.aozorabunko_repo_path is None:
        args.aozorabunko_repo_path = os.environ["AOZORABUNKO_REPO_PATH"]

    logger.setLevel(args.log_level.upper())
    with logging_redirect_tqdm(loggers=[logger]):
        main(**vars(args))
