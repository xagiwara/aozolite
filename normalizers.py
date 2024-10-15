import unicodedata

hiragana2katakana = {chr(x): chr(x + 0x30A1 - 0x3041) for x in range(0x3041, 0x3097)}
normalize_katakana = {
    "ァ": "ア",
    "ィ": "イ",
    "ゥ": "ウ",
    "ェ": "エ",
    "ォ": "オ",
    "ッ": "ツ",
    "ャ": "ヤ",
    "ュ": "ユ",
    "ョ": "ヨ",
    "ヵ": "カ",
    "ヶ": "ケ",
    "ヰ": "イ",
    "ヱ": "エ",
    "ヲ": "オ",
    "ヂ": "ジ",
    "ヅ": "ズ",
    "ヴァ": "バ",
    "ヴィ": "ビ",
    "ヴ": "ブ",
    "ヴェ": "ベ",
    "ヴォ": "ボ",
}
charset = set(hiragana2katakana.values()) | set([f"{x}" for x in range(10)])


def normalize_reading(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    for k, v in hiragana2katakana.items():
        text = text.replace(k, v)
    for k, v in normalize_katakana.items():
        text = text.replace(k, v)
    text = "".join([x for x in text if x in charset])
    text = (
        unicodedata.normalize("NFKD", text).replace("\u3099", "").replace("\u309a", "")
    )
    return unicodedata.normalize("NFKC", text)