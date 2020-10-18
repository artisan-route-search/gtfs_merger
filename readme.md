# GTFS merger

複数のGTFSに対し、stop_idやtrip_idなどに一意のidを振り直し、複数のGTFSを1つにまとめるプログラム。

## Usage
1. 複数のGTFSをGTFSディレクトリに配置する。
2. ``go run main.go``コマンドを実行
3. 結合された結果である``GTFS.zip``が生成される

## To do list
これからやるべきことは次の通り。

- [ ] GTFS-RTとの互換性を持たせる為、置換前のidと置換後のidを変換するテーブルを作成し、GTFS-RTのIDを置き換えるプログラムを作成する

- [ ] GTFS-JPへの対応