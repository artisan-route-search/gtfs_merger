# GTFS merger

複数のGTFS及びGTFS-JPに対し、stop_idやtrip_idなどに一意のidを振り直し、複数のGTFSを1つにまとめるプログラム。

## Env
- [Golang](https://golang.org/) ver.1.13以上

## Usage
1. 複数のGTFSをGTFSディレクトリに配置する。
2. ``go run main.go``コマンドを実行
（GTFS-JPの場合には``go run main.go -e=jp``）
3. 結合された結果である``GTFS.zip``が生成される

## To do list
追加・改良したいこと

- [ ] GTFS-RTとの互換性を持たせる為、置換前のidと置換後のidを変換するテーブルを作成し、GTFS-RTのIDを置き換えるプログラムを作成する

- [ ] 結合時、GTFS及びGTFS-JPの仕様を満たしているかチェックする機能