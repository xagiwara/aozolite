from bs4 import BeautifulSoup
from typing import NamedTuple, Literal
import re
from normalizers import normalize_text

# パーサof図書カード


class CardInfo(NamedTuple):
    categories: list[int]
    style: str
    note: str | None
    first: str | None


class TitleInfo(NamedTuple):
    title: str
    title_reading: str
    subtitle: str | None
    subtitle_reading: str | None
    original_title: str | None
    anthology: str | None
    anthology_reading: str | None
    author: int


class AuthorInfo(NamedTuple):
    aozora_id: int
    type: Literal["author", "editor", "translator"]
    name: str
    name_reading: str
    name_roman: str
    birth: str | None
    death: str | None


class CardData(NamedTuple):
    title: TitleInfo
    info: CardInfo
    authors: list[AuthorInfo]


person_pattern = r".*/person(\d+)\.html$"


def find_person(elm: BeautifulSoup):
    author_id = int(
        [
            x
            for x in [
                re.match(person_pattern, x.attrs["href"]) for x in elm.find_all("a")
            ]
            if x is not None
        ][0][1]
    )
    return author_id


def parse_card(path: str):
    with open(path, "rb") as f:
        soup = BeautifulSoup(f, "html.parser")

    title_elm = soup.find("table", attrs={"summary": "タイトルデータ"})
    values = {
        "subtitle": None,
        "subtitle_reading": None,
        "original_title": None,
        "anthology": None,
        "anthology_reading": None,
    }

    trs = title_elm.find_all("tr")
    for tr in trs:
        tds = tr.find_all("td")
        k = tds[0].text.strip()
        if k == "作品名：":
            values["title"] = normalize_text(tds[1].text)
        elif k == "作品名読み：":
            values["title_reading"] = normalize_text(tds[1].text)
        elif k == "副題：":
            values["subtitle"] = normalize_text(tds[1].text)
        elif k == "副題読み：":
            values["subtitle_reading"] = normalize_text(tds[1].text)
        elif k == "原題：":
            values["original_title"] = normalize_text(tds[1].text)
        elif k == "作品集名：":
            values["anthology"] = normalize_text(tds[1].text)
        elif k == "作品集名読み：":
            values["anthology_reading"] = normalize_text(tds[1].text)
        elif k == "著者名：":
            values["author"] = find_person(tds[1])
        else:
            raise ValueError(f"Unknown key: {k}")

    title_info = TitleInfo(**values)

    data_items = soup.find("table", attrs={"summary": "作品データ"})
    values = {
        "categories": [],
        "note": None,
        "first": None,
    }

    trs = data_items.find_all("tr")
    for tr in trs:
        tds = tr.find_all("td")
        if tds[0].text == "分類：":
            c = tds[1].text.lstrip("NDC").strip()
            values["categories"] = [int(x.lstrip("K")) for x in c.split(" ") if x != ""]
        elif tds[0].text == "文字遣い種別：":
            values["style"] = tds[1].text
        elif tds[0].text == "備考：":
            values["note"] = tds[1].get_text()
        elif tds[0].text == "初出：":
            values["first"] = tds[1].text
        elif tds[0].text == "作品について：":
            pass
        else:
            raise ValueError(f"Unknown key: {tds[0].text}")

    if values["note"] is not None:
        values["note"] = (
            values["note"] if values["note"].replace("\n", "").strip() != "" else None
        )

    card_info = CardInfo(**values)

    authors = []

    author_elms = soup.find_all("table", attrs={"summary": "作家データ"})
    for author_elm in author_elms:
        # author id
        author_id = find_person(author_elm)

        values = {"aozora_id": author_id, "death": None, "birth": None}
        trs = author_elm.find_all("tr")
        for tr in trs:
            tds = tr.find_all("td")
            if tds[0].text == "分類：":
                if tds[1].text == "著者":
                    values["type"] = "author"
                elif tds[1].text == "編者":
                    values["type"] = "editor"
                elif tds[1].text == "校訂者":
                    values["type"] = "proofreader"
                elif tds[1].text == "翻訳者":
                    values["type"] = "translator"
                elif tds[1].text == "その他":
                    values["type"] = "other"
                else:
                    raise ValueError(f"Unknown type: {tds[1].text}")
            elif tds[0].text == "作家名：":
                values["name"] = normalize_text(tds[1].text)
            elif tds[0].text == "作家名読み：":
                values["name_reading"] = normalize_text(tds[1].get_text())
            elif tds[0].text == "ローマ字表記：":
                values["name_roman"] = normalize_text(tds[1].text)
            elif tds[0].text == "生年：":
                values["birth"] = tds[1].text
            elif tds[0].text == "没年：":
                values["death"] = tds[1].text
            elif tds[0].text == "人物について：":
                pass
            else:
                raise ValueError(f"Unknown key: {tds[0].text}")

        authors.append(AuthorInfo(**values))

    return CardData(title=title_info, info=card_info, authors=authors)
