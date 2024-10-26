from bs4 import BeautifulSoup
from typing import Literal, NamedTuple
import copy
from normalizers import normalize_text
import re
import os
from logging import getLogger
import sqlite3

logger = getLogger(__name__)
logger.setLevel("INFO")

# パーサof図書


class BookTextData(NamedTuple):
    body_raw: str
    body_text_rb_major: str
    body_text_rt_major: str
    colophon_raw: str
    colophon_text: str
    license: str | None


unicode_pattern = re.compile(r"U\+([0-9A-F]+)")


def extract_text(main_text: BeautifulSoup, major: Literal["rb", "rt"]) -> str:
    main_text = copy.deepcopy(main_text)

    for ruby in main_text.find_all("ruby"):
        ruby.replace_with(ruby.find(major).text)

    for gaiji in main_text.find_all("img", class_="gaiji"):
        alt: str = gaiji.attrs["alt"]
        unicode_match = re.search(unicode_pattern, alt)
        if unicode_match:
            gaiji.replace_with(chr(int(unicode_match.group(1), 16)))
        else:
            gaiji.replace_with("\uFFFD")

    for img in main_text.find_all("img"):
        img.replace_with("")

    for note in main_text.find_all("span", class_="notes"):
        note.replace_with("")

    for br in main_text.find_all(string="\n"):
        br.replace_with("")

    for br in main_text.find_all("br"):
        br.replace_with("\n")

    text = main_text.get_text()
    return normalize_text(text)


def extract_colophon(soup: BeautifulSoup) -> tuple[str, str]:
    bi = soup.find("div", class_="bibliographical_information")
    at = soup.find("div", class_="after_text")

    if bi is None and at is None:
        raise ValueError("colophon not found")

    if bi is not None and at is not None:
        raise ValueError("multiple colophons found")

    colophon = bi or at
    raw_colophon = colophon.encode("utf-8").decode("utf-8")
    colophon_text = "\n".join(
        [x.strip() for x in colophon.get_text().split("\n") if x.strip() != ""]
    )
    return raw_colophon, colophon_text


def extract_license(soup: BeautifulSoup) -> str | None:
    license = soup.find("a", rel="license")
    if license is None:
        return None
    return license.attrs["href"]


def parse_book(path: str):
    with open(path, "r", encoding="cp932") as f:
        soup = BeautifulSoup(f, "html.parser")

    main_text = soup.find("div", class_="main_text")
    if main_text is None:
        return None

    body_raw = main_text.encode("utf-8").decode("utf-8")
    body_text_rb_major = extract_text(main_text, "rb")
    body_text_rt_major = extract_text(main_text, "rt")
    colophon_raw, colophon_text = extract_colophon(soup)
    license = extract_license(soup)

    return BookTextData(
        body_raw=body_raw,
        body_text_rb_major=body_text_rb_major,
        body_text_rt_major=body_text_rt_major,
        colophon_raw=colophon_raw,
        colophon_text=colophon_text,
        license=license,
    )


book_file_pattern = re.compile(r"^.*/([0-9]{6})/files/([0-9]+)_([0-9]+)\.html$")


def find_files(repo_path: str):
    html_files: list[(int, int, int)] = []
    for root, _, files in os.walk(os.path.join(repo_path, "cards")):
        if os.path.basename(root) != "files":
            continue
        for file in [os.path.join(root, x) for x in files]:
            m = book_file_pattern.search(file.replace("\\", "/"))
            if m is None:
                continue

            html_files += [(int(m.group(1)), int(m.group(2)), int(m.group(3)))]
    return html_files


def create_tables(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS book_texts (
            book_id INTEGER NOT NULL,
            revision INTEGER NOT NULL,
            body_raw BLOB NOT NULL,
            body_text_rb_major TEXT NOT NULL,
            body_text_rt_major TEXT NOT NULL,
            colophon_raw BLOB NOT NULL,
            colophon_text TEXT NOT NULL,
            license TEXT,
            FOREIGN KEY (book_id) REFERENCES books(id),
            UNIQUE (book_id, revision)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_book_texts_book_id ON book_texts(book_id)"
    )


if __name__ == "__main__":
    html_files = find_files(os.environ["AOZORABUNKO_REPO_PATH"])
    print(f"found {len(html_files)} files")
