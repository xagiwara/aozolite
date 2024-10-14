from bs4 import BeautifulSoup
import unicodedata
from typing import Literal, NamedTuple
import copy

# パーサof図書


class BookData(NamedTuple):
    raw_body: str
    body_text_rb_major: str
    body_text_rt_major: str
    footnote_raw: str
    footnote_text: str
    license: str | None


def extract_text(main_text: BeautifulSoup, major: Literal["rb", "rt"]) -> str:
    main_text = copy.deepcopy(main_text)

    replace_texts = {
        "※": "\uFFFD",  # U+FFFD: REPLACEMENT CHARACTER
        "／＼": "\u3031",  # U+3031: VERTICAL KANA REPEAT MARK
        "／″＼": "\u3032",  # U+3032: VERTICAL KANA REPEAT WITH VOICED SOUND MARK
        "\n": "",
        "\r": "",
    }

    for ruby in main_text.find_all("ruby"):
        ruby.replace_with(ruby.find(major).text)

    for text, replace in replace_texts.items():
        for e in main_text.find_all(string=text):
            e.replace_with(replace)

    for gaiji in main_text.find_all("img", class_="gaiji"):
        gaiji.replace_with("\uFFFD")

    for img in main_text.find_all("img"):
        img.replace_with("")

    for note in main_text.find_all("span", class_="notes"):
        note.replace_with("")

    for br in main_text.find_all("br"):
        br.replace_with("\n")

    text = main_text.get_text()
    text = "\n".join([x.strip() for x in text.split("\n") if x.strip() != ""])
    text = unicodedata.normalize("NFKC", text)
    return text


def extract_footnote(soup: BeautifulSoup) -> str:
    bi = soup.find("div", class_="bibliographical_information")
    at = soup.find("div", class_="after_text")

    if bi is None and at is None:
        raise ValueError("footnote not found")

    if bi is not None and at is not None:
        raise ValueError("multiple footnotes found")

    footnote = bi or at
    raw_footnote = footnote.encode("utf-8").decode("utf-8")
    footnote_text = "\n".join(
        [x.strip() for x in footnote.get_text().split("\n") if x.strip() != ""]
    )
    return raw_footnote, footnote_text


def extract_license(soup: BeautifulSoup) -> str | None:
    license = soup.find("a", rel="license")
    if license is None:
        return None
    return license.attrs["href"]


def parse_book(path: str):
    with open(path, "rb") as f:
        soup = BeautifulSoup(f, "html.parser")

    main_text = soup.find("div", class_="main_text")
    if main_text is None:
        return None

    raw_body = main_text.encode("utf-8").decode("utf-8")
    body_text_rb_major = extract_text(main_text, "rb")
    body_text_rt_major = extract_text(main_text, "rt")
    footnote_raw, footnote_text = extract_footnote(soup)
    license = extract_license(soup)

    return BookData(
        raw_body=raw_body,
        body_text_rb_major=body_text_rb_major,
        body_text_rt_major=body_text_rt_major,
        footnote_raw=footnote_raw,
        footnote_text=footnote_text,
        license=license,
    )