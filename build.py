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
    BookTextData,
)
import shutil
import multiprocessing as mp
from multiprocessing.pool import AsyncResult


STYLE_VERSION = "2.0.0"

logger = getLogger(__name__)


class BookMetadata(NamedTuple):
    title: str
    author: str
    editor: str | None
    original_title: str | None
    subtitle: str | None
    translator: str | None


def main(
    aozorabunko_repo_path: str, output_dir: str, temp_dir: str, procs: int, **kwargs
):
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
    total_book_files = len(book_files)

    logger.info(f"found {total_book_files} files")
    c.execute("SELECT book_id, revision FROM book_texts")
    processed_texts: list[tuple[int, int]] = c.fetchall()
    logger.info(f"found {len(processed_texts)} processed texts")
    book_files = [
        (
            author_id,
            book_id,
            version_id,
            os.path.join(
                "cards", f"{author_id:06}", "files", f"{book_id}_{version_id}.html"
            ),
        )
        for (author_id, book_id, version_id) in book_files
        if (book_id, version_id) not in processed_texts
    ]
    logger.info(f"found {len(book_files)} new files")
    tasks: dict[int, AsyncResult] = {}

    logger.info("writing book texts...")
    with mp.Pool(procs - 1) as pool:
        for i, (_, book_id, version_id, filepath) in enumerate(book_files):
            tasks[i] = pool.apply_async(
                func=parse_book, args=(os.path.join(aozorabunko_repo_path, filepath),)
            )

        with tqdm(total=total_book_files, initial=len(processed_texts)) as pbar:
            while len(tasks) > 0:
                for i in list(tasks.keys()):
                    if tasks[i].ready():
                        try:
                            (_, book_id, version_id, book_filepath) = book_files[i]

                            book: BookTextData | None = tasks[i].get()
                            if book is None:
                                logger.info(
                                    f"skipped: {book_filepath} (reason: body not found)"
                                )
                                continue

                            # CC ライセンスに ND もしくは SA を含むならスキップ
                            if book.license is not None:
                                cc = [
                                    x.lower()
                                    for x in book.license.split("/")[4].split("-")
                                ]
                                if "nd" in cc or "sa" in cc:
                                    logger.info(
                                        f"skipped: {book_filepath} (reason: CC {cc})"
                                    )
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
                            conn.commit()
                        except Exception as e:
                            logger.error(f"skipped: {book_filepath}")
                            logger.error(e)
                            continue
                        finally:
                            pbar.update(1)
                            del tasks[i]

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
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("OUTPUT_DIR"),
        required=not os.environ.get("OUTPUT_DIR"),
    )
    parser.add_argument(
        "--temp_dir",
        default=os.environ.get("TEMP_DIR", os.path.join(gettempdir(), "aozolite")),
    )
    parser.add_argument(
        "--aozorabunko-repo-path",
        default=os.environ.get("AOZORABUNKO_REPO_PATH"),
        required=not os.environ.get("AOZORABUNKO_REPO_PATH"),
    )
    parser.add_argument(
        "--procs",
        type=int,
        default=os.environ.get("PROCS", (os.cpu_count() or 1) + 1),
    )
    args = parser.parse_args()

    logger.setLevel(args.log_level.upper())
    with logging_redirect_tqdm(loggers=[logger]):
        logger.info(args)
        main(**vars(args))
