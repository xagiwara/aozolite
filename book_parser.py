from bs4 import BeautifulSoup
import unicodedata
from typing import Literal, NamedTuple
import copy

# パーサof図書


class BookData(NamedTuple):
    body_raw: str
    body_text_rb_major: str
    body_text_rt_major: str
    colophon_raw: str
    colophon_text: str
    license: str | None


def extract_text(main_text: BeautifulSoup, major: Literal["rb", "rt"]) -> str:
    main_text = copy.deepcopy(main_text)

    replace_texts = {
        "※": "\uFFFD",  # U+FFFD: REPLACEMENT CHARACTER
        "／＼": "\u3031",  # U+3031: VERTICAL KANA REPEAT MARK
        "／″＼": "\u3032",  # U+3032: VERTICAL KANA REPEAT WITH VOICED SOUND MARK
    }

    for ruby in main_text.find_all("ruby"):
        ruby.replace_with(ruby.find(major).text)

    for gaiji in main_text.find_all("img", class_="gaiji"):
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
    text = unicodedata.normalize("NFC", text)

    for key, replace in replace_texts.items():
        text = text.replace(key, replace)

    text = "\n".join([x.strip() for x in text.split("\n") if x.strip() != ""])
    text = unicodedata.normalize("NFKC", text)
    return text


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

    return BookData(
        body_raw=body_raw,
        body_text_rb_major=body_text_rb_major,
        body_text_rt_major=body_text_rt_major,
        colophon_raw=colophon_raw,
        colophon_text=colophon_text,
        license=license,
    )
