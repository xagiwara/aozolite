# 青空文庫を sqlite に押し込める

## 必要なもの

- `git`

## 準備

```sh
git clone --filter=tree:0 https://github.com/aozorabunko/aozorabunko.git
```

ディレクトリを環境変数 `AOZORABUNKO_REPO_PATH` で指定

出力先は環境変数 `OUTPUT_PATH` で指定

TODO: `argparse` 使いたい

## 備考

- CC ライセンス（著作権有効）かつ、ND（改変禁止）を含む場合はスキップする
- CC ライセンス（著作権有効）かつ、SA（継承）を含む場合はスキップする
